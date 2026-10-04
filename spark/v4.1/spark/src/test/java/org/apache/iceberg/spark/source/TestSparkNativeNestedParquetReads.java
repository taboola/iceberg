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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkException;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestSparkNativeNestedParquetReads extends TestBaseWithCatalog {
  private static final String FLAG = SparkSQLProperties.PARQUET_SPARK_NATIVE_NESTED_ENABLED;
  private static final Map<String, String> NATIVE_READS_ON = ImmutableMap.of(FLAG, "true");
  private static final int BATCH_SIZE = 2;
  private static final String SOURCE_TABLE = "spark_catalog.default.source";
  private static final String EVENTS_TYPE =
      "array<struct<ts bigint, tags array<struct<k string, v double>>, attrs map<string, struct<n int>>>>";
  private static final String NESTED_PROJECTION =
      "struct<id:bigint,events:array<struct<ts:bigint,tags:array<struct<k:string,v:double>>>>,loc:struct<lat:double>>";

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", SOURCE_TABLE);
  }

  @TestTemplate
  void nestedProjectionUsesSparkReader() {
    createNestedTable();

    assertReaderFactory(NESTED_PROJECTION, SparkNativeParquetReaderFactory.class);
    assertNativePlanMatches(
        "SELECT id, events.ts AS ts, events.tags AS tags, loc.lat AS lat FROM %s ORDER BY id");
    assertNativeReadMatches("SELECT * FROM %s ORDER BY id");
  }

  @TestTemplate
  void flatProjectionKeepsIcebergVectorizedReader() {
    createNestedTable();

    assertReaderFactory("struct<id:bigint,name:string>", SparkColumnarReaderFactory.class);
  }

  @TestTemplate
  void disabledByDefault() {
    createNestedTable();

    assertThat(readerFactory(NESTED_PROJECTION)).isInstanceOf(SparkRowReaderFactory.class);
  }

  @TestTemplate
  void fallsBackWhenSparkVectorizedReaderIsOff() {
    createNestedTable();

    withSQLConf(
        nativeReadsWith("spark.sql.parquet.enableVectorizedReader", "false"),
        () ->
            assertThat(readerFactory(NESTED_PROJECTION)).isInstanceOf(SparkRowReaderFactory.class));
  }

  @TestTemplate
  void fallsBackWhenSparkNestedVectorizationIsOff() {
    createNestedTable();

    withSQLConf(
        nativeReadsWith("spark.sql.parquet.enableNestedColumnVectorizedReader", "false"),
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

    assertReaderFactory(NESTED_PROJECTION, SparkRowReaderFactory.class);
    assertNativeReadMatches("SELECT id, events.ts AS ts FROM %s ORDER BY id");
  }

  @TestTemplate
  void fallsBackForMetadataColumn() {
    createNestedTable();

    assertReaderFactory(
        "struct<_file:string,events:array<struct<ts:bigint>>>", SparkRowReaderFactory.class);
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

    assertReaderFactory(NESTED_PROJECTION, SparkRowReaderFactory.class);
  }

  @TestTemplate
  void fallsBackForTypeSparkReadsDifferently() {
    validationCatalog.createTable(
        tableIdent,
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "s", Types.StructType.of(optional(3, "u", Types.UUIDType.get())))),
        PartitionSpec.unpartitioned());

    assertReaderFactory("struct<id:bigint,s:struct<u:string>>", SparkRowReaderFactory.class);
  }

  @TestTemplate
  void schemaEvolutionResolvesByFieldId() {
    createNestedTable();
    sql("ALTER TABLE %s RENAME COLUMN events.element.ts TO at", tableName);
    // the new ts column takes the old name of the renamed column but has its own ID
    sql(
        "ALTER TABLE %s ADD COLUMNS (events.element.extra int, events.element.ts int, loc.alt double, note string)",
        tableName);
    sql("ALTER TABLE %s ALTER COLUMN note FIRST", tableName);
    sql("ALTER TABLE %s ALTER COLUMN events.element.extra FIRST", tableName);
    sql("ALTER TABLE %s DROP COLUMN name", tableName);
    sql("INSERT INTO %s (id, note) VALUES (9, 'n')", tableName);

    assertReaderFactory(
        "struct<id:bigint,events:array<struct<at:bigint,extra:int,ts:int>>,loc:struct<alt:double>,note:string>",
        SparkNativeParquetReaderFactory.class);
    assertNativeReadMatches(
        "SELECT id, events.at AS at, events.extra AS extra, events.ts AS ts, loc.alt AS alt, note FROM %s ORDER BY id");
    assertNativeReadMatches("SELECT * FROM %s ORDER BY id");
  }

  @TestTemplate
  void typePromotionResolvesByFieldId() {
    sql(
        "CREATE TABLE %s (id int, n int, f float, d1 decimal(9,2), d2 decimal(9,2), s struct<m int, p decimal(18,4)>) USING iceberg",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, 10, 1.5, 12.34, 56.78, named_struct('m', 3, 'p', 1.2345))",
        tableName);
    sql("ALTER TABLE %s ALTER COLUMN n TYPE bigint", tableName);
    sql("ALTER TABLE %s ALTER COLUMN f TYPE double", tableName);
    // int, long and fixed-length binary decimals
    sql("ALTER TABLE %s ALTER COLUMN d1 TYPE decimal(12,2)", tableName);
    sql("ALTER TABLE %s ALTER COLUMN d2 TYPE decimal(20,2)", tableName);
    sql("ALTER TABLE %s ALTER COLUMN s.m TYPE bigint", tableName);
    sql("ALTER TABLE %s ALTER COLUMN s.p TYPE decimal(38,4)", tableName);
    sql(
        "INSERT INTO %s VALUES (2, 20, 2.5, 1234567890.12, 123456789012345678.12, named_struct('m', 4, 'p', 12345678901234567890.1234))",
        tableName);

    assertReaderFactory(
        "struct<id:int,n:bigint,f:double,d1:decimal(12,2),d2:decimal(20,2),s:struct<m:bigint,p:decimal(38,4)>>",
        SparkNativeParquetReaderFactory.class);
    assertNativeReadMatches("SELECT * FROM %s ORDER BY id");
  }

  @TestTemplate
  void identityPartitionColumnIsReadFromTheFile() {
    sql(
        "CREATE TABLE %s (id bigint, dt string, events array<struct<ts bigint>>) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, 'd1', array(named_struct('ts', 10L))), (2, 'd2', array(named_struct('ts', 20L), named_struct('ts', CAST(NULL AS bigint)))), (3, 'd1', CAST(NULL AS array<struct<ts bigint>>))",
        tableName);

    assertReaderFactory(
        "struct<id:bigint,dt:string,events:array<struct<ts:bigint>>>",
        SparkNativeParquetReaderFactory.class);
    assertNativeReadMatches("SELECT id, dt, events.ts AS ts FROM %s ORDER BY id");
  }

  @TestTemplate
  void filesWithoutFieldIdsFailInsteadOfReadingNulls() {
    // Hive-style Parquet files carry neither field IDs nor the identity partition column
    sql(
        "CREATE TABLE %s (id bigint, events array<struct<ts bigint>>, dt string) USING parquet PARTITIONED BY (dt)",
        SOURCE_TABLE);
    sql(
        "INSERT INTO %s VALUES (1, array(named_struct('ts', 10L)), 'd1'), (2, array(), 'd2')",
        SOURCE_TABLE);
    sql(
        "CREATE TABLE %s (id bigint, events array<struct<ts bigint>>, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql(
        "CALL %s.system.add_files(table => '%s', source_table => '%s')",
        catalogName, tableName, SOURCE_TABLE);
    String projection = "struct<id:bigint,events:array<struct<ts:bigint>>,dt:string>";
    String query = "SELECT id, events.ts AS ts, dt FROM %s ORDER BY id";

    // the name mapping that add_files sets keeps the scan on the Iceberg readers
    assertReaderFactory(projection, SparkRowReaderFactory.class);
    assertNativeReadMatches(query);

    sql(
        "ALTER TABLE %s UNSET TBLPROPERTIES ('%s')",
        tableName, TableProperties.DEFAULT_NAME_MAPPING);
    assertReaderFactory(projection, SparkNativeParquetReaderFactory.class);
    withSQLConf(
        NATIVE_READS_ON,
        () ->
            assertThatThrownBy(() -> sql(query, tableName))
                .isInstanceOf(SparkException.class)
                .hasMessageContaining("Parquet file schema doesn't contain any field Ids"));
  }

  @TestTemplate
  void resolvingFileIOOverHadoopUsesSparkReader() throws Exception {
    createNestedTable();
    String prefix = "spark.sql.catalog." + catalogName + ".";
    spark.conf().set("spark.sql.catalog.resolving", SparkCatalog.class.getName());
    catalogConfig.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog.resolving." + key, value));
    if (spark.conf().contains(prefix + "warehouse")) {
      spark
          .conf()
          .set("spark.sql.catalog.resolving.warehouse", spark.conf().get(prefix + "warehouse"));
    }

    spark.conf().set("spark.sql.catalog.resolving.io-impl", ResolvingFileIO.class.getName());
    Table table = Spark3Util.loadIcebergTable(spark, "resolving.default.table");
    assertThat(table.io()).isInstanceOf(ResolvingFileIO.class);

    withSQLConf(
        NATIVE_READS_ON,
        () ->
            assertThat(scan(table, NESTED_PROJECTION).toBatch().createReaderFactory())
                .isInstanceOf(SparkNativeParquetReaderFactory.class));
    String query =
        "SELECT id, events.ts AS ts, loc.lat AS lat FROM resolving.default.table ORDER BY id";
    List<Object[]> expected = sql(query);
    withSQLConf(NATIVE_READS_ON, () -> assertEquals(query, expected, sql(query)));
  }

  @TestTemplate
  void filtersAreAppliedOnTheNativePath() {
    createNestedTable();

    assertNativePlanMatches(
        "SELECT id, events.ts AS ts FROM %s WHERE id > 1 AND (name = 'd' OR loc.lon > 3.0) ORDER BY id");
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
    Map<String, String> conf =
        nativeReadsWith(SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_ENABLED, "false");

    withSQLConf(
        conf,
        () -> {
          Batch batch = scan("struct<id:bigint,events:array<struct<ts:bigint>>>").toBatch();
          assertThat(batch.createReaderFactory())
              .isInstanceOf(SparkNativeParquetReaderFactory.class);
          assertThat(batch.planInputPartitions().length).isGreaterThan(1);
        });
    assertNativeReadMatches(conf, "SELECT id, events.ts AS ts FROM %s ORDER BY id");
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

    assertReaderFactory(
        "struct<id:int,f:float,d:date,ts:timestamp,tsn:timestamp_ntz,dec:decimal(9,2),bigdec:decimal(20,4),bin:binary,s:struct<n:int>>",
        SparkNativeParquetReaderFactory.class);
    assertNativeReadMatches("SELECT * FROM %s ORDER BY id");
  }

  @TestTemplate
  void offHeapVectorsMatchTheRowReader() {
    createNestedTable();

    assertNativeReadMatches(
        nativeReadsWith("spark.sql.columnVector.offheap.enabled", "true"),
        "SELECT * FROM %s ORDER BY id");
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
          int batches = 0;
          long rows = 0;
          for (InputPartition partition : batch.planInputPartitions()) {
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
        });
  }

  @TestTemplate
  void rowReadsAreNotSupported() {
    createNestedTable();

    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          Batch batch = scan(NESTED_PROJECTION).toBatch();
          PartitionReaderFactory factory = batch.createReaderFactory();
          InputPartition partition = batch.planInputPartitions()[0];
          assertThatThrownBy(() -> factory.createReader(partition))
              .isInstanceOf(UnsupportedOperationException.class)
              .hasMessage("Row-based reads are not supported");
        });
  }

  @TestTemplate
  void metadataTablesAndChangelogKeepIcebergReaders() {
    createNestedTable();

    assertNativeReadMatches("SELECT file_path, record_count FROM %s.files ORDER BY file_path");
    assertNativeReadMatches("SELECT id, _change_type FROM %s.changes ORDER BY id");
  }

  @TestTemplate
  void requestedSchemaCarriesFieldIdsAndSyntheticNames() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "events",
                Types.ListType.ofOptional(
                    3,
                    Types.StructType.of(
                        optional(4, "ts", Types.LongType.get()),
                        optional(
                            5,
                            "attrs",
                            Types.MapType.ofOptional(
                                6,
                                7,
                                Types.StringType.get(),
                                Types.StructType.of(
                                    optional(8, "n", Types.IntegerType.get()))))))));

    StructType requested = SparkNativeParquetReaderFactory.requestedSchema(schema);

    StructField id = requested.fields()[0];
    assertThat(fieldId(id)).isEqualTo(1L);
    assertThat(id.name()).matches("_1_[0-9a-f]{8}");
    assertThat(id.dataType()).isEqualTo(DataTypes.LongType);
    assertThat(id.nullable()).isFalse();

    StructField events = requested.fields()[1];
    assertThat(fieldId(events)).isEqualTo(2L);
    StructType element = (StructType) ((ArrayType) events.dataType()).elementType();
    assertThat(fieldIds(element)).containsExactly(4L, 5L);
    DataType value = ((MapType) element.fields()[1].dataType()).valueType();
    assertThat(fieldIds((StructType) value)).containsExactly(8L);
    assertThat(Arrays.stream(element.fields()).map(StructField::name))
        .allMatch(name -> name.matches("_\\d+_[0-9a-f]{8}"));
  }

  private static long fieldId(StructField field) {
    return field.metadata().getLong(ParquetUtils.FIELD_ID_METADATA_KEY());
  }

  private static List<Long> fieldIds(StructType struct) {
    return Arrays.stream(struct.fields()).map(TestSparkNativeNestedParquetReads::fieldId).toList();
  }

  private static Map<String, String> nativeReadsWith(String key, String value) {
    return ImmutableMap.of(FLAG, "true", key, value);
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

  private void assertReaderFactory(String projection, Class<?> expected) {
    withSQLConf(
        NATIVE_READS_ON, () -> assertThat(readerFactory(projection)).isInstanceOf(expected));
  }

  private void assertNativeReadMatches(String query) {
    assertNativeReadMatches(NATIVE_READS_ON, query);
  }

  private void assertNativeReadMatches(Map<String, String> conf, String query) {
    String statement = String.format(query, tableName);
    List<Object[]> expected = sql(statement);
    withSQLConf(conf, () -> assertEquals(query, expected, sql(statement)));
  }

  private void assertNativePlanMatches(String query) {
    String statement = String.format(query, tableName);
    List<Object[]> expected = sql(statement);
    withSQLConf(
        NATIVE_READS_ON,
        () -> {
          SparkPlan plan = executeAndKeepPlan(() -> assertEquals(query, expected, sql(statement)));
          assertThat(plan.toString()).contains("ColumnarToRow");
        });
  }

  private PartitionReaderFactory readerFactory(String projection) {
    return scan(projection).toBatch().createReaderFactory();
  }

  private Scan scan(String projection) {
    return scan(validationCatalog.loadTable(tableIdent), projection);
  }

  private Scan scan(Table table, String projection) {
    SparkScanBuilder builder = new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    builder.pruneColumns((StructType) DataType.fromDDL(projection));
    return builder.build();
  }
}
