/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.ImmutableOrcBatchReadConf;
import org.apache.iceberg.spark.ImmutableParquetBatchReadConf;
import org.apache.iceberg.spark.OrcBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkBatch implements Batch {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatch.class);
  // Spark has no time type, maps UUID to string and has no reader for the rest
  private static final Set<Type.TypeID> SPARK_UNSUPPORTED_TYPES =
      EnumSet.of(
          Type.TypeID.TIME,
          Type.TypeID.UUID,
          Type.TypeID.VARIANT,
          Type.TypeID.UNKNOWN,
          Type.TypeID.GEOMETRY,
          Type.TypeID.GEOGRAPHY,
          Type.TypeID.TIMESTAMP_NANO);

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final Table table;
  private final Supplier<FileIO> fileIO;
  private final SparkReadConf readConf;
  private final Types.StructType groupingKeyType;
  private final List<? extends ScanTaskGroup<?>> taskGroups;
  private final Schema projection;
  private final boolean caseSensitive;
  private final boolean localityEnabled;
  private final boolean executorCacheLocalityEnabled;
  private final int scanHashCode;
  private final boolean cacheDeleteFilesOnExecutors;

  SparkBatch(
      SparkSession spark,
      JavaSparkContext sparkContext,
      Table table,
      Supplier<FileIO> fileIO,
      SparkReadConf readConf,
      Types.StructType groupingKeyType,
      List<? extends ScanTaskGroup<?>> taskGroups,
      Schema projection,
      int scanHashCode) {
    this.spark = spark;
    this.sparkContext = sparkContext;
    this.table = table;
    this.fileIO = fileIO;
    this.readConf = readConf;
    this.groupingKeyType = groupingKeyType;
    this.taskGroups = taskGroups;
    this.projection = projection;
    this.caseSensitive = readConf.caseSensitive();
    this.localityEnabled = readConf.localityEnabled();
    this.executorCacheLocalityEnabled = readConf.executorCacheLocalityEnabled();
    this.scanHashCode = scanHashCode;
    this.cacheDeleteFilesOnExecutors = readConf.cacheDeleteFilesOnExecutors();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast =
        sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    Broadcast<FileIO> fileIOBroadcast =
        sparkContext.broadcast(SerializableFileIOWithSize.wrap(fileIO.get()));
    String projectionString = SchemaParser.toJson(projection);
    String[][] locations = computePreferredLocations();

    InputPartition[] partitions = new InputPartition[taskGroups.size()];

    for (int index = 0; index < taskGroups.size(); index++) {
      partitions[index] =
          new SparkInputPartition(
              groupingKeyType,
              taskGroups.get(index),
              tableBroadcast,
              fileIOBroadcast,
              projectionString,
              caseSensitive,
              locations != null ? locations[index] : SparkPlanningUtil.NO_LOCATION_PREFERENCE,
              cacheDeleteFilesOnExecutors);
    }

    return partitions;
  }

  private String[][] computePreferredLocations() {
    if (localityEnabled) {
      return SparkPlanningUtil.fetchBlockLocations(fileIO.get(), taskGroups);

    } else if (executorCacheLocalityEnabled) {
      List<String> executorLocations = SparkUtil.executorLocations();
      if (!executorLocations.isEmpty()) {
        return SparkPlanningUtil.assignExecutors(taskGroups, executorLocations);
      }
    }

    return null;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    if (useParquetBatchReads()) {
      return new SparkColumnarReaderFactory(parquetBatchReadConf());

    } else if (useSparkNativeParquetReads()) {
      return new SparkNativeParquetReaderFactory(spark, projection, readConf.parquetBatchSize());

    } else if (useOrcBatchReads()) {
      return new SparkColumnarReaderFactory(orcBatchReadConf());

    } else {
      return new SparkRowReaderFactory();
    }
  }

  private ParquetBatchReadConf parquetBatchReadConf() {
    return ImmutableParquetBatchReadConf.builder().batchSize(readConf.parquetBatchSize()).build();
  }

  private OrcBatchReadConf orcBatchReadConf() {
    return ImmutableOrcBatchReadConf.builder().batchSize(readConf.orcBatchSize()).build();
  }

  // conditions for using Parquet batch reads:
  // - Parquet vectorization is enabled
  // - only primitives or metadata columns are projected
  // - all tasks are of FileScanTask type and read only Parquet files
  private boolean useParquetBatchReads() {
    return readConf.parquetVectorizationEnabled()
        && projection.columns().stream().allMatch(this::supportsParquetBatchReads)
        && taskGroups.stream().allMatch(this::supportsParquetBatchReads);
  }

  private boolean supportsParquetBatchReads(ScanTask task) {
    if (task instanceof ScanTaskGroup) {
      ScanTaskGroup<?> taskGroup = (ScanTaskGroup<?>) task;
      return taskGroup.tasks().stream().allMatch(this::supportsParquetBatchReads);

    } else if (task.isFileScanTask() && !task.isDataTask()) {
      FileScanTask fileScanTask = task.asFileScanTask();
      return fileScanTask.file().format() == FileFormat.PARQUET;

    } else {
      return false;
    }
  }

  private boolean supportsParquetBatchReads(Types.NestedField field) {
    return field.type().isPrimitiveType() || MetadataColumns.isMetadataColumn(field.fieldId());
  }

  // conditions for reading nested projections with Spark's vectorized Parquet reader:
  // - the session flag is on and Parquet vectorization is enabled
  // - the table has no name mapping (files are matched by field ID) and uses HadoopFileIO
  // - the projection has no metadata columns, initial defaults or types Spark cannot read
  // - Spark's own vectorized reader switches allow the schema
  // - all tasks read Parquet data files without delete files or encryption
  private boolean useSparkNativeParquetReads() {
    if (!readConf.parquetSparkNativeNestedEnabled()) {
      return false;
    }

    String blocker = sparkNativeReadsBlocker();
    if (blocker != null) {
      LOG.info("[NATIVE-NESTED] Not used for {}: {}", table.name(), blocker);
      return false;
    }

    LOG.info(
        "[NATIVE-NESTED] Reading {} ({} task groups) with Spark's vectorized Parquet reader",
        table.name(),
        taskGroups.size());
    return true;
  }

  private String sparkNativeReadsBlocker() {
    if (!readConf.parquetVectorizationEnabled()) {
      return "Parquet vectorization is disabled";
    }

    if (table.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
      return "the table has a name mapping";
    }

    FileIO io = fileIO.get();
    if (!(io instanceof HadoopFileIO)) {
      return "the FileIO is " + io.getClass().getName() + ", not HadoopFileIO";
    }

    for (Types.NestedField column : projection.columns()) {
      String unsupported = unsupportedColumn(column);
      if (unsupported != null) {
        return unsupported;
      }
    }

    SQLConf conf = spark.sessionState().conf();
    if (!ParquetUtils.isBatchReadSupportedForSchema(conf, SparkSchemaUtil.convert(projection))) {
      return "Spark's vectorized Parquet reader is disabled for this schema";
    }

    for (ScanTaskGroup<?> taskGroup : taskGroups) {
      String unsupported = unsupportedTask(taskGroup);
      if (unsupported != null) {
        return unsupported;
      }
    }

    return null;
  }

  private String unsupportedTask(ScanTask task) {
    if (task instanceof ScanTaskGroup) {
      for (ScanTask child : ((ScanTaskGroup<?>) task).tasks()) {
        String unsupported = unsupportedTask(child);
        if (unsupported != null) {
          return unsupported;
        }
      }

      return null;
    }

    if (!supportsParquetBatchReads(task)) {
      return "the scan has a task that is not a Parquet data file scan";
    }

    FileScanTask fileScanTask = task.asFileScanTask();
    if (!fileScanTask.deletes().isEmpty()) {
      return "file " + fileScanTask.file().location() + " has delete files";
    }

    if (fileScanTask.file().keyMetadata() != null) {
      return "file " + fileScanTask.file().location() + " is encrypted";
    }

    return null;
  }

  private String unsupportedColumn(Types.NestedField field) {
    String columnName = projection.findColumnName(field.fieldId());
    String name = columnName != null ? columnName : field.name();
    if (MetadataColumns.isMetadataColumn(field.fieldId())) {
      return "column " + name + " is a metadata column";
    }

    // Iceberg returns the default for files written before the column was added
    if (field.initialDefault() != null) {
      return "column " + name + " has an initial default value";
    }

    Type type = field.type();
    if (type.isNestedType()) {
      for (Types.NestedField child : type.asNestedType().fields()) {
        String unsupported = unsupportedColumn(child);
        if (unsupported != null) {
          return unsupported;
        }
      }

      return null;
    }

    return SPARK_UNSUPPORTED_TYPES.contains(type.typeId())
        ? "column " + name + " has type " + type + ", which Spark's reader cannot read"
        : null;
  }

  // conditions for using ORC batch reads:
  // - ORC vectorization is enabled
  // - all tasks are of type FileScanTask and read only ORC files with no delete files
  private boolean useOrcBatchReads() {
    return readConf.orcVectorizationEnabled()
        && taskGroups.stream().allMatch(this::supportsOrcBatchReads);
  }

  private boolean supportsOrcBatchReads(ScanTask task) {
    if (task instanceof ScanTaskGroup) {
      ScanTaskGroup<?> taskGroup = (ScanTaskGroup<?>) task;
      return taskGroup.tasks().stream().allMatch(this::supportsOrcBatchReads);

    } else if (task.isFileScanTask() && !task.isDataTask()) {
      FileScanTask fileScanTask = task.asFileScanTask();
      return fileScanTask.file().format() == FileFormat.ORC && fileScanTask.deletes().isEmpty();

    } else {
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatch that = (SparkBatch) o;
    return table.name().equals(that.table.name()) && scanHashCode == that.scanHashCode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(table.name(), scanHashCode);
  }
}
