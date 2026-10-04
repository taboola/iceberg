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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.metrics.TaskNumDeletes;
import org.apache.iceberg.spark.source.metrics.TaskNumSplits;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;
import org.apache.spark.sql.internal.LegacyBehaviorPolicy;
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
 * vectorized reader, decodes nested columns. Columns are resolved by Iceberg field ID at every
 * nesting level. Used only for scans that {@link SparkBatch} selects for it.
 */
class SparkNativeParquetReaderFactory implements PartitionReaderFactory {
  private static final String REBASE_MODE = LegacyBehaviorPolicy.CORRECTED().toString();

  private final String requestedSchemaJson;
  private final int batchSize;
  private final String sessionTimeZone;
  private final boolean offHeap;

  SparkNativeParquetReaderFactory(Schema projection, ParquetBatchReadConf conf) {
    SQLConf sqlConf = SQLConf.get();
    this.requestedSchemaJson = requestedSchema(projection).json();
    this.batchSize = conf.batchSize();
    this.sessionTimeZone = sqlConf.sessionLocalTimeZone();
    this.offHeap = sqlConf.offHeapColumnVectorEnabled();
  }

  static boolean supportsProjection(Schema projection) {
    return ParquetUtils.isBatchReadSupportedForSchema(
        SQLConf.get(), SparkSchemaUtil.convert(projection));
  }

  // Spark clips the file schema by field ID, but its schema converter then maps the clipped file
  // columns onto the requested fields by name before falling back to the ID, so the requested
  // schema carries the Iceberg IDs under names that cannot occur in a file; batches are positional
  // and the scan's output schema still comes from the Iceberg projection
  static StructType requestedSchema(Schema projection) {
    String nonce = UUID.randomUUID().toString().substring(0, 8);
    return (StructType) TypeUtil.visit(projection, new RequestedSchemaVisitor(nonce));
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
    Preconditions.checkArgument(
        partition.allTasksOfType(FileScanTask.class),
        "Unsupported task group for columnar reads: %s",
        partition.taskGroup());
    return new BatchReader(partition);
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return true;
  }

  // the FileIO configuration is what Iceberg opens the same files with; on executors the FileIO
  // is a serializable wrapper that exposes the configuration of the Hadoop-backed FileIO checked
  // on the driver, and the per-scan keys go on a copy so the broadcast configuration stays
  // untouched
  private Configuration readerConf(FileIO io, boolean caseSensitive) {
    Preconditions.checkArgument(
        io instanceof HadoopConfigurable, "FileIO %s has no Hadoop configuration", io);
    Configuration fileIOConf = ((HadoopConfigurable) io).getConf();
    Preconditions.checkNotNull(fileIOConf, "FileIO %s has no Hadoop configuration", io);
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

  private static class RequestedSchemaVisitor extends TypeUtil.SchemaVisitor<DataType> {
    private final String nonce;

    private RequestedSchemaVisitor(String nonce) {
      this.nonce = nonce;
    }

    @Override
    public DataType schema(Schema schema, DataType structType) {
      return structType;
    }

    @Override
    public DataType struct(Types.StructType struct, List<DataType> fieldTypes) {
      List<Types.NestedField> fields = struct.fields();
      StructField[] sparkFields = new StructField[fields.size()];
      for (int i = 0; i < sparkFields.length; i += 1) {
        Types.NestedField field = fields.get(i);
        Metadata metadata =
            new MetadataBuilder()
                .putLong(ParquetUtils.FIELD_ID_METADATA_KEY(), field.fieldId())
                .build();
        sparkFields[i] =
            new StructField(
                "_" + field.fieldId() + "_" + nonce,
                fieldTypes.get(i),
                field.isOptional(),
                metadata);
      }

      return new StructType(sparkFields);
    }

    @Override
    public DataType field(Types.NestedField field, DataType fieldType) {
      return fieldType;
    }

    @Override
    public DataType list(Types.ListType list, DataType elementType) {
      return new ArrayType(elementType, list.isElementOptional());
    }

    @Override
    public DataType map(Types.MapType map, DataType keyType, DataType valueType) {
      return new MapType(keyType, valueType, map.isValueOptional());
    }

    @Override
    public DataType variant(Types.VariantType variant) {
      return SparkSchemaUtil.convert(variant);
    }

    @Override
    public DataType primitive(Type.PrimitiveType primitive) {
      return SparkSchemaUtil.convert(primitive);
    }
  }

  private class BatchReader extends BaseReader<ColumnarBatch, FileScanTask>
      implements PartitionReader<ColumnarBatch> {
    private final TaskAttemptContext context;
    private final long numSplits;

    private BatchReader(SparkInputPartition partition) {
      super(
          partition.table(),
          partition.io(),
          partition.taskGroup(),
          partition.projection(),
          partition.isCaseSensitive(),
          partition.cacheDeleteFilesOnExecutors());
      this.context =
          new TaskAttemptContextImpl(
              readerConf(partition.io(), partition.isCaseSensitive()), new TaskAttemptID());
      this.numSplits = partition.taskGroup().tasks().size();
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues() {
      return new CustomTaskMetric[] {new TaskNumSplits(numSplits), new TaskNumDeletes(0)};
    }

    @Override
    protected Stream<ContentFile<?>> referencedFiles(FileScanTask task) {
      return Stream.of(task.file());
    }

    @Override
    protected CloseableIterator<ColumnarBatch> open(FileScanTask task) {
      String location = task.file().location();
      InputFileBlockHolder.set(location, task.start(), task.length());
      // Iceberg writes proleptic Gregorian dates and timestamps, so nothing is rebased
      VectorizedParquetRecordReader reader =
          new VectorizedParquetRecordReader(
              null,
              REBASE_MODE,
              sessionTimeZone,
              REBASE_MODE,
              sessionTimeZone,
              offHeap && TaskContext.get() != null,
              batchSize);
      try {
        reader.initialize(
            new FileSplit(new Path(location), task.start(), task.length(), new String[0]), context);
      } catch (IOException e) {
        closeQuietly(reader);
        throw new UncheckedIOException("Failed to open " + location, e);
      } catch (InterruptedException e) {
        closeQuietly(reader);
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while opening " + location, e);
      }

      reader.initBatch(new StructType(), InternalRow.empty());
      reader.enableReturningBatches();
      return new BatchIterator(reader);
    }
  }

  private static void closeQuietly(VectorizedParquetRecordReader reader) {
    try {
      reader.close();
    } catch (IOException ignored) {
      // the open failure is the error to report
    }
  }

  private static class BatchIterator implements CloseableIterator<ColumnarBatch> {
    private final VectorizedParquetRecordReader reader;
    private boolean advanced = false;
    private boolean hasMore = false;

    private BatchIterator(VectorizedParquetRecordReader reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      if (!advanced) {
        try {
          this.hasMore = reader.nextBatch();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        this.advanced = true;
      }

      return hasMore;
    }

    @Override
    public ColumnarBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      this.advanced = false;
      return reader.resultBatch();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
