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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkNativeNestedParquetReads extends TestBaseWithCatalog {
  private static final Map<String, String> NATIVE_READS_ON =
      ImmutableMap.of(SparkSQLProperties.PARQUET_SPARK_NATIVE_NESTED_ENABLED, "true");
  private static final int BATCH_SIZE = 2;
  private static final String EVENTS_TYPE =
      "array<struct<ts bigint, tags array<struct<k string, v double>>, attrs map<string, struct<n int>>>>";
  private static final String NESTED_PROJECTION =
      "struct<id:bigint,events:array<struct<ts:bigint,tags:array<struct<k:string,v:double>>>>,loc:struct<lat:double>>";

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @BeforeEach
  void useCatalog() {
    sql("USE %s", catalogName);
  }

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void nestedProjectionUsesSparkReader() {
    createNestedTable();
    String nested =
        String.format(
            "SELECT id, events.ts AS ts, events.tags AS tags, loc.lat AS lat FROM %s ORDER BY id",
            tableName);
    String all = String.format("SELECT * FROM %s ORDER BY id", tableName);
    List<Object[]> expectedNested = sql(nested);
    List<Object[]> expectedAll = sql(all);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          assertThat(readerFactory(NESTED_PROJECTION))
              .isInstanceOf(SparkNativeParquetReaderFactory.class);
          Dataset<Row> nestedRows = spark.sql(nested);
          assertEquals("Nested projection", expectedNested, rowsToJava(nestedRows.collectAsList()));
          assertThat(nestedRows.queryExecution().executedPlan().toString())
              .contains("ColumnarToRow");
          assertEquals("Full projection", expectedAll, sql(all));
        });
  }

  @TestTemplate
  void flatProjectionKeepsIcebergVectorizedReader() {
    createNestedTable();

    withSQLConf(
        NATIVE_READS_ON,
        () ->
            assertThat(readerFactory("struct<id:bigint,name:string>"))
                .isInstanceOf(SparkColumnarReaderFactory.class));
  }

  @TestTemplate
  void disabledByDefault() {
    createNestedTable();

    assertThat(readerFactory(NESTED_PROJECTION)).isInstanceOf(SparkRowReaderFactory.class);
  }

  @TestTemplate
  void fallsBackWhenSparkVectorizationIsOff() {
    createNestedTable();

    withSQLConf(
        ImmutableMap.<String, String>builder()
            .putAll(NATIVE_READS_ON)
            .put("spark.sql.parquet.enableVectorizedReader", "false")
            .buildOrThrow(),
        () ->
            assertThat(readerFactory(NESTED_PROJECTION)).isInstanceOf(SparkRowReaderFactory.class));

    withSQLConf(
        ImmutableMap.<String, String>builder()
            .putAll(NATIVE_READS_ON)
            .put("spark.sql.parquet.enableNestedColumnVectorizedReader", "false")
            .buildOrThrow(),
        () ->
            assertThat(readerFactory(NESTED_PROJECTION)).isInstanceOf(SparkRowReaderFactory.class));
  }

  @TestTemplate
  void fallsBackForDeleteFiles() {
    createNestedTable();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='merge-on-read')",
        tableName, TableProperties.DELETE_MODE);
    sql("DELETE FROM %s WHERE id = 2", tableName);
    String query = String.format("SELECT id, events.ts AS ts FROM %s ORDER BY id", tableName);
    List<Object[]> expected = sql(query);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          assertThat(readerFactory(NESTED_PROJECTION)).isInstanceOf(SparkRowReaderFactory.class);
          assertEquals("Rows after delete", expected, sql(query));
        });
  }

  @TestTemplate
  void fallsBackForMetadataColumn() {
    createNestedTable();

    withSQLConf(
        NATIVE_READS_ON,
        () ->
            assertThat(readerFactory("struct<_file:string,events:array<struct<ts:bigint>>>"))
                .isInstanceOf(SparkRowReaderFactory.class));
  }

  @TestTemplate
  void fallsBackForNameMapping() {
    createNestedTable();
    Table table = validationCatalog.loadTable(tableIdent);
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(table.schema())))
        .commit();

    withSQLConf(
        NATIVE_READS_ON,
        () ->
            assertThat(readerFactory(NESTED_PROJECTION)).isInstanceOf(SparkRowReaderFactory.class));
  }

  @TestTemplate
  void fallsBackForTypeSparkCannotRead() {
    validationCatalog.createTable(
        tableIdent,
        new Schema(
            required(1, "id", LongType.get()),
            optional(2, "s", StructType.of(optional(3, "u", UUIDType.get())))),
        PartitionSpec.unpartitioned());

    withSQLConf(
        NATIVE_READS_ON,
        () ->
            assertThat(readerFactory("struct<id:bigint,s:struct<u:string>>"))
                .isInstanceOf(SparkRowReaderFactory.class));
  }

  @TestTemplate
  void schemaEvolutionResolvesByFieldId() {
    createNestedTable();
    sql("ALTER TABLE %s RENAME COLUMN events.element.ts TO at", tableName);
    sql(
        "ALTER TABLE %s ADD COLUMNS (events.element.extra int, loc.alt double, note string)",
        tableName);
    sql("ALTER TABLE %s ALTER COLUMN note FIRST", tableName);
    sql("ALTER TABLE %s ALTER COLUMN events.element.extra FIRST", tableName);
    sql("ALTER TABLE %s DROP COLUMN name", tableName);
    sql("INSERT INTO %s (id, note) VALUES (9, 'n')", tableName);
    String query =
        String.format(
            "SELECT id, events.at AS at, events.extra AS extra, loc.alt AS alt, note FROM %s ORDER BY id",
            tableName);
    String all = String.format("SELECT * FROM %s ORDER BY id", tableName);
    List<Object[]> expected = sql(query);
    List<Object[]> expectedAll = sql(all);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          assertThat(
                  readerFactory(
                      "struct<id:bigint,events:array<struct<at:bigint,extra:int>>,loc:struct<alt:double>,note:string>"))
              .isInstanceOf(SparkNativeParquetReaderFactory.class);
          assertEquals("Rows across schema versions", expected, sql(query));
          assertEquals("Reordered columns", expectedAll, sql(all));
        });
  }

  @TestTemplate
  void identityPartitionColumnIsReadFromTheFile() {
    sql(
        "CREATE TABLE %s (id bigint, dt string, events array<struct<ts bigint>>) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, 'd1', array(named_struct('ts', 10L))), (2, 'd2', array(named_struct('ts', 20L), named_struct('ts', CAST(NULL AS bigint)))), (3, 'd1', CAST(NULL AS array<struct<ts bigint>>))",
        tableName);
    String query = String.format("SELECT id, dt, events.ts AS ts FROM %s ORDER BY id", tableName);
    List<Object[]> expected = sql(query);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          assertThat(readerFactory("struct<id:bigint,dt:string,events:array<struct<ts:bigint>>>"))
              .isInstanceOf(SparkNativeParquetReaderFactory.class);
          assertEquals("Partitioned rows", expected, sql(query));
        });
  }

  @TestTemplate
  void filtersAreAppliedOnTheNativePath() {
    createNestedTable();
    String query =
        String.format(
            "SELECT id, events.ts AS ts FROM %s WHERE id > 1 AND (name = 'd' OR loc.lon > 3.0) ORDER BY id",
            tableName);
    List<Object[]> expected = sql(query);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          Dataset<Row> rows = spark.sql(query);
          assertEquals("Filtered rows", expected, rowsToJava(rows.collectAsList()));
          assertThat(rows.queryExecution().executedPlan().toString()).contains("ColumnarToRow");
        });
  }

  @TestTemplate
  void fileSplitIntoSeveralTasks() {
    sql(
        "CREATE TABLE %s (id bigint, events array<struct<ts bigint>>) USING iceberg "
            + "TBLPROPERTIES ('%s'='1', '%s'='1', '%s'='2', '%s'='1024', '%s'='4')",
        tableName,
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
        TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
        TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
        TableProperties.SPLIT_SIZE,
        TableProperties.PARQUET_BATCH_SIZE);
    sql("INSERT INTO %s SELECT id, array(named_struct('ts', id * 10)) FROM range(20)", tableName);
    String query = String.format("SELECT id, events.ts AS ts FROM %s ORDER BY id", tableName);
    List<Object[]> expected = sql(query);
    assertThat(expected).hasSize(20);

    withSQLConf(
        ImmutableMap.<String, String>builder()
            .putAll(NATIVE_READS_ON)
            .put(SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_ENABLED, "false")
            .buildOrThrow(),
        () -> {
          Batch batch = scan("struct<id:bigint,events:array<struct<ts:bigint>>>").toBatch();
          assertThat(batch.createReaderFactory())
              .isInstanceOf(SparkNativeParquetReaderFactory.class);
          assertThat(batch.planInputPartitions().length).isGreaterThan(1);
          assertEquals("Rows across splits", expected, sql(query));
        });
  }

  @TestTemplate
  void primitiveTypesMatchTheRowReader() {
    sql(
        "CREATE TABLE %s (id int, f float, d date, ts timestamp, tsn timestamp_ntz, "
            + "dec decimal(9,2), bigdec decimal(20,4), bin binary, s struct<n int>) USING iceberg",
        tableName);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 1.5, DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.123456', "
            + "TIMESTAMP_NTZ '2024-01-02 03:04:05.123456', 12.34, 123456789012345.6789, X'0102', named_struct('n', 7)), "
            + "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, named_struct('n', CAST(NULL AS int)))",
        tableName);
    String query = String.format("SELECT * FROM %s ORDER BY id", tableName);
    List<Object[]> expected = sql(query);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          assertThat(
                  readerFactory(
                      "struct<id:int,f:float,d:date,ts:timestamp,tsn:timestamp_ntz,dec:decimal(9,2),bigdec:decimal(20,4),bin:binary,s:struct<n:int>>"))
              .isInstanceOf(SparkNativeParquetReaderFactory.class);
          assertEquals("All primitive types", expected, sql(query));
        });
  }

  @TestTemplate
  void offHeapVectorsMatchTheRowReader() {
    createNestedTable();
    String query = String.format("SELECT * FROM %s ORDER BY id", tableName);
    List<Object[]> expected = sql(query);

    withSQLConf(
        ImmutableMap.<String, String>builder()
            .putAll(NATIVE_READS_ON)
            .put("spark.sql.columnVector.offheap.enabled", "true")
            .buildOrThrow(),
        () -> assertEquals("Off-heap rows", expected, sql(query)));
  }

  @TestTemplate
  void typePromotionResolvesByFieldId() {
    sql("CREATE TABLE %s (id int, n int, f float, s struct<m int>) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 10, 1.5, named_struct('m', 3))", tableName);
    sql("ALTER TABLE %s ALTER COLUMN n TYPE bigint", tableName);
    sql("ALTER TABLE %s ALTER COLUMN f TYPE double", tableName);
    sql("ALTER TABLE %s ALTER COLUMN s.m TYPE bigint", tableName);
    sql("INSERT INTO %s VALUES (2, 20, 2.5, named_struct('m', 4))", tableName);
    String query = String.format("SELECT * FROM %s ORDER BY id", tableName);
    List<Object[]> expected = sql(query);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          assertThat(readerFactory("struct<id:int,n:bigint,f:double,s:struct<m:bigint>>"))
              .isInstanceOf(SparkNativeParquetReaderFactory.class);
          assertEquals("Promoted columns", expected, sql(query));
        });
  }

  @TestTemplate
  void batchesSpanRowsAndFiles() {
    createNestedTable();
    long expectedRows = 0;
    int expectedBatches = 0;
    try (CloseableIterable<FileScanTask> tasks =
        validationCatalog.loadTable(tableIdent).newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        long count = task.file().recordCount();
        expectedRows += count;
        expectedBatches += (int) ((count + BATCH_SIZE - 1) / BATCH_SIZE);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    assertThat(expectedRows).isEqualTo(5);
    long rowCount = expectedRows;
    int batchCount = expectedBatches;

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          Batch batch = scan("struct<id:bigint,events:array<struct<ts:bigint>>>").toBatch();
          PartitionReaderFactory factory = batch.createReaderFactory();
          InputPartition[] partitions = batch.planInputPartitions();
          int batches = 0;
          long rows = 0;
          for (InputPartition partition : partitions) {
            assertThat(factory.supportColumnarReads(partition)).isTrue();
            try (PartitionReader<ColumnarBatch> reader = factory.createColumnarReader(partition)) {
              while (reader.next()) {
                batches += 1;
                rows += reader.get().numRows();
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }

          assertThat(rows).isEqualTo(rowCount);
          assertThat(batches).isEqualTo(batchCount);
          assertThatThrownBy(() -> factory.createReader(partitions[0]))
              .isInstanceOf(UnsupportedOperationException.class)
              .hasMessage("Row-based reads are not supported");
        });
  }

  @TestTemplate
  void metadataTablesAndChangelogKeepIcebergReaders() {
    createNestedTable();
    String files =
        String.format("SELECT file_path, record_count FROM %s.files ORDER BY file_path", tableName);
    String changes =
        String.format("SELECT id, _change_type FROM %s.changes ORDER BY id", tableName);
    List<Object[]> expectedFiles = sql(files);
    List<Object[]> expectedChanges = sql(changes);

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          assertEquals("Files metadata table", expectedFiles, sql(files));
          assertEquals("Changelog rows", expectedChanges, sql(changes));
        });
  }

  @TestTemplate
  void requestedSchemaCarriesFieldIdsAndSyntheticNames() {
    Schema schema =
        new Schema(
            required(1, "id", LongType.get()),
            optional(
                2,
                "events",
                ListType.ofOptional(
                    3,
                    StructType.of(
                        optional(4, "ts", LongType.get()),
                        optional(
                            5,
                            "attrs",
                            MapType.ofOptional(
                                6,
                                7,
                                StringType.get(),
                                StructType.of(optional(8, "n", IntegerType.get()))))))));

    org.apache.spark.sql.types.StructType requested =
        SparkNativeParquetReaderFactory.requestedSchema(schema);

    List<Long> ids = Lists.newArrayList();
    List<String> names = Lists.newArrayList();
    collect(requested, ids, names);
    assertThat(ids).containsExactly(1L, 2L, 4L, 5L, 8L);
    assertThat(names).allMatch(name -> name.matches("_\\d+_[0-9a-f]{8}"));
    assertThat(requested.fields()[0].dataType()).isEqualTo(DataTypes.LongType);
    assertThat(requested.fields()[1].dataType()).isInstanceOf(ArrayType.class);
  }

  private static void collect(
      org.apache.spark.sql.types.StructType struct, List<Long> ids, List<String> names) {
    for (StructField field : struct.fields()) {
      ids.add(field.metadata().getLong(ParquetUtils.FIELD_ID_METADATA_KEY()));
      names.add(field.name());
      collect(field.dataType(), ids, names);
    }
  }

  private static void collect(DataType type, List<Long> ids, List<String> names) {
    if (type instanceof org.apache.spark.sql.types.StructType) {
      collect((org.apache.spark.sql.types.StructType) type, ids, names);
    } else if (type instanceof ArrayType) {
      collect(((ArrayType) type).elementType(), ids, names);
    } else if (type instanceof org.apache.spark.sql.types.MapType) {
      collect(((org.apache.spark.sql.types.MapType) type).valueType(), ids, names);
    }
  }

  private void createNestedTable() {
    sql(
        "CREATE TABLE %s (id bigint, name string, events %s, loc struct<lat double, lon double>) USING iceberg TBLPROPERTIES ('format-version'='2', '%s'='%d')",
        tableName, EVENTS_TYPE, TableProperties.PARQUET_BATCH_SIZE, BATCH_SIZE);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'a', array(named_struct('ts', 10L, 'tags', array(named_struct('k', 'x', 'v', 1.5D), named_struct('k', 'y', 'v', CAST(NULL AS double))), 'attrs', map('m', named_struct('n', 1)))), named_struct('lat', 1.0D, 'lon', 2.0D)), "
            + "(2, NULL, CAST(array() AS %s), NULL), "
            + "(3, 'c', NULL, named_struct('lat', CAST(NULL AS double), 'lon', 3.0D))",
        tableName, EVENTS_TYPE);
    sql(
        "INSERT INTO %s VALUES "
            + "(4, 'd', array(named_struct('ts', CAST(NULL AS bigint), 'tags', CAST(array() AS array<struct<k string, v double>>), 'attrs', CAST(map() AS map<string, struct<n int>>)), named_struct('ts', 40L, 'tags', CAST(NULL AS array<struct<k string, v double>>), 'attrs', CAST(NULL AS map<string, struct<n int>>))), named_struct('lat', 4.0D, 'lon', 4.5D)), "
            + "(5, 'e', CAST(array() AS %s), named_struct('lat', 5.0D, 'lon', 5.5D))",
        tableName, EVENTS_TYPE);
  }

  private PartitionReaderFactory readerFactory(String projection) {
    return scan(projection).toBatch().createReaderFactory();
  }

  private Scan scan(String projection) {
    Table table = validationCatalog.loadTable(tableIdent);
    SparkScanBuilder builder = new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    builder.pruneColumns((org.apache.spark.sql.types.StructType) DataType.fromDDL(projection));
    return builder.build();
  }
}
