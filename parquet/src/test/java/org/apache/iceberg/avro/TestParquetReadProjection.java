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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestParquetReadProjection extends TestReadProjection {
  @Override
  protected GenericData.Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, GenericData.Record record)
      throws IOException {
    File file = temp.resolve(desc + ".parquet").toFile();
    file.delete();

    try (FileAppender<GenericData.Record> appender =
        Parquet.write(Files.localOutput(file)).schema(writeSchema).build()) {
      appender.add(record);
    }

    Iterable<GenericData.Record> records =
        Parquet.read(Files.localInput(file)).project(readSchema).callInit().build();

    return Iterables.getOnlyElement(records);
  }

  @Test
  void nestedProjectionInListsAndStructs() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "s",
                Types.StructType.of(
                    Types.NestedField.optional(3, "inner", pointType(4, 5)),
                    Types.NestedField.optional(
                        6, "points", Types.ListType.ofRequired(7, pointType(8, 9))),
                    Types.NestedField.optional(10, "z", Types.StringType.get()))),
            Types.NestedField.optional(
                11,
                "matrix",
                Types.ListType.ofRequired(12, Types.ListType.ofRequired(13, pointType(14, 15)))));

    Schema projection =
        writeSchema.select("id", "s.inner.x", "s.points.element.y", "matrix.element.element.x");

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(writeSchema, "table");
    org.apache.avro.Schema structSchema =
        AvroSchemaUtil.fromOption(avroSchema.getField("s").schema());
    org.apache.avro.Schema innerSchema =
        AvroSchemaUtil.fromOption(structSchema.getField("inner").schema());
    org.apache.avro.Schema pointSchema =
        AvroSchemaUtil.fromOption(structSchema.getField("points").schema()).getElementType();
    org.apache.avro.Schema cellSchema =
        AvroSchemaUtil.fromOption(avroSchema.getField("matrix").schema())
            .getElementType()
            .getElementType();

    GenericData.Record struct = new GenericData.Record(structSchema);
    struct.put("inner", point(innerSchema, 1, 2L));
    struct.put("points", List.of(point(pointSchema, 3, 4L), point(pointSchema, 5, 6L)));
    struct.put("z", "z");

    GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("id", 34L);
    record.put("s", struct);
    record.put(
        "matrix",
        List.of(
            List.of(point(cellSchema, 7, 8L)),
            List.of(point(cellSchema, 9, 10L), point(cellSchema, 11, 12L))));

    GenericData.Record projected = writeAndRead("nested_lists", writeSchema, projection, record);

    assertThat(projected.get("id")).isEqualTo(record.get("id"));

    GenericData.Record projectedStruct = (GenericData.Record) projected.get("s");
    assertThat(fieldNames(projectedStruct)).containsExactly("inner", "points");

    GenericData.Record projectedInner = (GenericData.Record) projectedStruct.get("inner");
    assertThat(fieldNames(projectedInner)).containsExactly("x");
    assertThat(projectedInner.get("x"))
        .isEqualTo(((GenericData.Record) struct.get("inner")).get("x"));

    assertThat(records(projectedStruct.get("points")))
        .allSatisfy(point -> assertThat(fieldNames(point)).containsExactly("y"))
        .extracting(point -> point.get("y"))
        .isEqualTo(records(struct.get("points")).stream().map(point -> point.get("y")).toList());

    List<List<GenericData.Record>> projectedMatrix = recordLists(projected.get("matrix"));
    List<List<GenericData.Record>> matrix = recordLists(record.get("matrix"));
    assertThat(projectedMatrix).hasSameSizeAs(matrix);
    for (int i = 0; i < matrix.size(); i += 1) {
      assertThat(projectedMatrix.get(i))
          .allSatisfy(cell -> assertThat(fieldNames(cell)).containsExactly("x"))
          .extracting(cell -> cell.get("x"))
          .isEqualTo(matrix.get(i).stream().map(cell -> cell.get("x")).toList());
    }
  }

  private static Types.StructType pointType(int xId, int yId) {
    return Types.StructType.of(
        Types.NestedField.optional(xId, "x", Types.IntegerType.get()),
        Types.NestedField.optional(yId, "y", Types.LongType.get()));
  }

  private static GenericData.Record point(org.apache.avro.Schema schema, int xValue, long yValue) {
    GenericData.Record point = new GenericData.Record(schema);
    point.put("x", xValue);
    point.put("y", yValue);
    return point;
  }

  private static List<String> fieldNames(GenericData.Record record) {
    return record.getSchema().getFields().stream().map(Field::name).toList();
  }

  @SuppressWarnings("unchecked")
  private static List<GenericData.Record> records(Object list) {
    return (List<GenericData.Record>) list;
  }

  @SuppressWarnings("unchecked")
  private static List<List<GenericData.Record>> recordLists(Object list) {
    return (List<List<GenericData.Record>>) list;
  }

  @Override
  @Test
  @Disabled("Empty struct read is not supported for Parquet")
  public void testEmptyStructProjection() throws Exception {}

  @Override
  @Test
  @Disabled("Empty struct read is not supported for Parquet")
  public void testEmptyStructRequiredProjection() throws Exception {}

  @Override
  @Test
  @Disabled("Empty struct read is not supported for Parquet")
  public void testRequiredEmptyStructInRequiredStruct() throws Exception {}

  @Override
  @Test
  @Disabled("Empty struct read is not supported for Parquet")
  public void testEmptyNestedStructProjection() throws Exception {}

  @Override
  @Test
  @Disabled("Empty struct read is not supported for Parquet")
  public void testEmptyNestedStructRequiredProjection() throws Exception {}
}
