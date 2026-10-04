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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Reads Parquet scan tasks with Spark's vectorized Parquet reader, which, unlike Iceberg's
 * vectorized reader, decodes nested columns.
 *
 * <p>Columns are resolved by Iceberg field ID at every nesting level. Only used for scans that pass
 * the checks in {@link SparkBatch}: Parquet data files without deletes or encryption, a projection
 * without metadata columns, initial defaults or types Spark cannot represent, and a table whose
 * files are opened through Hadoop.
 */
class SparkNativeParquetReaderFactory implements PartitionReaderFactory {
  private static final String REBASE_MODE_CORRECTED = "CORRECTED";

  private final String requestedSchemaJson;
  private final boolean caseSensitive;
  private final int batchSize;
  private final String sessionTimeZone;
  private final boolean offHeap;

  SparkNativeParquetReaderFactory(SparkSession spark, Schema projection, int batchSize) {
    SQLConf conf = spark.sessionState().conf();
    this.requestedSchemaJson = requestedSchema(projection).json();
    this.caseSensitive = conf.caseSensitiveAnalysis();
    this.batchSize = batchSize;
    this.sessionTimeZone = conf.sessionLocalTimeZone();
    this.offHeap = conf.offHeapColumnVectorEnabled();
  }

  // Spark resolves a file column by name before falling back to its field ID, so the requested
  // schema carries the Iceberg IDs and names that cannot occur in a file; batches are positional
  // and the scan's output schema still comes from the Iceberg projection
  static StructType requestedSchema(Schema projection) {
    String nonce = UUID.randomUUID().toString().substring(0, 8);
    return withFieldIds(projection.asStruct(), SparkSchemaUtil.convert(projection), nonce);
  }

  private static StructType withFieldIds(
      Types.StructType struct, StructType sparkStruct, String nonce) {
    List<Types.NestedField> fields = struct.fields();
    StructField[] result = new StructField[fields.size()];
    for (int i = 0; i < result.length; i += 1) {
      Types.NestedField field = fields.get(i);
      StructField sparkField = sparkStruct.fields()[i];
      Metadata metadata =
          new MetadataBuilder()
              .withMetadata(sparkField.metadata())
              .putLong(ParquetUtils.FIELD_ID_METADATA_KEY(), field.fieldId())
              .build();
      result[i] =
          new StructField(
              "_" + field.fieldId() + "_" + nonce,
              withFieldIds(field.type(), sparkField.dataType(), nonce),
              sparkField.nullable(),
              metadata);
    }

    return new StructType(result);
  }

  private static DataType withFieldIds(Type type, DataType sparkType, String nonce) {
    if (type.isStructType()) {
      return withFieldIds(type.asStructType(), (StructType) sparkType, nonce);
    } else if (type.isListType()) {
      ArrayType array = (ArrayType) sparkType;
      return new ArrayType(
          withFieldIds(type.asListType().elementType(), array.elementType(), nonce),
          array.containsNull());
    } else if (type.isMapType()) {
      MapType map = (MapType) sparkType;
      return new MapType(
          withFieldIds(type.asMapType().keyType(), map.keyType(), nonce),
          withFieldIds(type.asMapType().valueType(), map.valueType(), nonce),
          map.valueContainsNull());
    }

    return sparkType;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    throw new UnsupportedOperationException("Row-based reads are not supported");
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition inputPartition) {
    Preconditions.checkArgument(
        inputPartition instanceof SparkInputPartition,
        "Unknown input partition type: %s",
        inputPartition.getClass().getName());
    SparkInputPartition partition = (SparkInputPartition) inputPartition;
    FileIO io = partition.io();
    Configuration conf =
        io instanceof HadoopConfigurable ? ((HadoopConfigurable) io).getConf() : null;
    Preconditions.checkNotNull(
        conf, "FileIO %s has no Hadoop configuration", io.getClass().getName());
    return new Reader(partition.<FileScanTask>taskGroup().tasks().iterator(), readerConf(conf));
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }

  // the FileIO configuration is what Iceberg opens the same files with; the per-scan keys go on
  // a copy so the broadcast configuration stays untouched
  private Configuration readerConf(Configuration fileIOConf) {
    Configuration conf = new Configuration(fileIOConf);
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, ParquetReadSupport.class.getName());
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), requestedSchemaJson);
    conf.setBoolean(SQLConf.CASE_SENSITIVE().key(), caseSensitive);
    conf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key(), false);
    conf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), true);
    conf.setBoolean(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED().key(), true);
    conf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG().key(), false);
    // a file without field IDs must fail instead of reading as nulls
    conf.setBoolean(SQLConf.PARQUET_FIELD_ID_READ_ENABLED().key(), true);
    conf.setBoolean(SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID().key(), false);
    return conf;
  }

  private class Reader implements PartitionReader<ColumnarBatch> {
    private final Iterator<FileScanTask> tasks;
    private final Configuration conf;
    private VectorizedParquetRecordReader current = null;

    Reader(Iterator<FileScanTask> tasks, Configuration conf) {
      this.tasks = tasks;
      this.conf = conf;
    }

    @Override
    public boolean next() throws IOException {
      while (true) {
        if (current != null && current.nextBatch()) {
          return true;
        }

        closeCurrent();
        if (!tasks.hasNext()) {
          return false;
        }

        open(tasks.next());
      }
    }

    @Override
    public ColumnarBatch get() {
      return current.resultBatch();
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
      InputFileBlockHolder.unset();
    }

    private void open(FileScanTask task) throws IOException {
      String location = task.file().location();
      InputFileBlockHolder.set(location, task.start(), task.length());
      // Iceberg writes proleptic Gregorian dates and timestamps, so nothing is rebased
      VectorizedParquetRecordReader reader =
          new VectorizedParquetRecordReader(
              null,
              REBASE_MODE_CORRECTED,
              sessionTimeZone,
              REBASE_MODE_CORRECTED,
              sessionTimeZone,
              offHeap && TaskContext.get() != null,
              batchSize);
      // assigned first so that close() also releases a reader that failed to open
      this.current = reader;
      try {
        reader.initialize(
            new FileSplit(new Path(location), task.start(), task.length(), new String[0]),
            new TaskAttemptContextImpl(conf, new TaskAttemptID()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }

      reader.initBatch(new StructType(), InternalRow.empty());
      reader.enableReturningBatches();
    }

    private void closeCurrent() throws IOException {
      if (current != null) {
        current.close();
        this.current = null;
      }
    }
  }
}
