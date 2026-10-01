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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestPruneColumns {
  @Test
  public void testMapKeyValueName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(2)
                                    .named("key"))
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(6)
                                            .named("z"))
                                    .id(3)
                                    .named("value"))
                            .named("custom_key_value_name"))
                    .as(LogicalTypeAnnotation.mapType())
                    .id(1)
                    .named("m"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "m",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    StructType.of(
                        NestedField.required(4, "x", DoubleType.get()),
                        NestedField.required(5, "y", DoubleType.get())))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(2)
                                    .named("key"))
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .id(3)
                                    .named("value"))
                            .named("custom_key_value_name"))
                    .as(LogicalTypeAnnotation.mapType())
                    .id(1)
                    .named("m"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should not rename repeated struct").isEqualTo(expected);
  }

  @Test
  public void testListElementName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(6)
                                            .named("z"))
                                    .id(3)
                                    .named("custom_element_name"))
                            .named("custom_repeated_name"))
                    .as(LogicalTypeAnnotation.listType())
                    .id(1)
                    .named("m"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "m",
                ListType.ofOptional(
                    3,
                    StructType.of(
                        NestedField.required(4, "x", DoubleType.get()),
                        NestedField.required(5, "y", DoubleType.get())))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .id(3)
                                    .named("custom_element_name"))
                            .named("custom_repeated_name"))
                    .as(LogicalTypeAnnotation.listType())
                    .id(1)
                    .named("m"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should not rename repeated struct").isEqualTo(expected);
  }

  @Test
  public void testStructElementName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(3)
                            .named("x"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(4)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(5)
                            .named("z"))
                    .id(2)
                    .named("struct_name_1"))
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(7)
                            .named("x"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(8)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(9)
                            .named("z"))
                    .id(6)
                    .named("struct_name_2"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                2,
                "struct_name_1",
                StructType.of(
                    NestedField.required(4, "y", DoubleType.get()),
                    NestedField.required(5, "z", DoubleType.get()))),
            NestedField.optional(6, "struct_name_2", StructType.of()));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(4)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(5)
                            .named("z"))
                    .id(2)
                    .named("struct_name_1"))
            .addField(Types.buildGroup(Type.Repetition.OPTIONAL).id(6).named("struct_name_2"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void testVariant() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(buildVariantType(2, "variant_1"))
            .addField(buildVariantType(3, "variant_2"))
            .named("table");

    Schema projection =
        new Schema(
            ImmutableList.of(
                NestedField.required(1, "id", IntegerType.get()),
                NestedField.required(2, "variant_1", VariantType.get())));
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(buildVariantType(2, "variant_1"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  void nestedStructInStruct() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2,
                        "inner",
                        StructType.of(
                            NestedField.optional(
                                3,
                                "deepest",
                                StructType.of(
                                    NestedField.optional(4, "x", IntegerType.get()),
                                    NestedField.optional(5, "y", IntegerType.get()))),
                            NestedField.optional(6, "z", IntegerType.get()))),
                    NestedField.optional(7, "w", IntegerType.get()))));

    assertPrunesTo(fileSchema, fileSchema.select("s.inner.deepest.x"));
  }

  @Test
  void listOfStructsInStruct() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(2, "l", ListType.ofOptional(3, pointType(4, 5))),
                    NestedField.optional(6, "z", IntegerType.get()))));

    assertPrunesTo(fileSchema, fileSchema.select("s.l.element.x"));
  }

  @Test
  void mapOfStructsInStruct() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2, "m", MapType.ofOptional(3, 4, StringType.get(), pointType(5, 6))),
                    NestedField.optional(7, "z", IntegerType.get()))));

    assertPrunesTo(fileSchema, fileSchema.select("s.m.value.x"));
  }

  @Test
  void nestedStructInListElement() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "l",
                ListType.ofOptional(
                    2,
                    StructType.of(
                        NestedField.optional(3, "inner", pointType(4, 5)),
                        NestedField.optional(6, "z", IntegerType.get())))));

    assertPrunesTo(fileSchema, fileSchema.select("l.element.inner.x"));
  }

  @Test
  void nestedStructInMapValue() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "m",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    StructType.of(
                        NestedField.optional(4, "inner", pointType(5, 6)),
                        NestedField.optional(7, "z", IntegerType.get())))));

    assertPrunesTo(fileSchema, fileSchema.select("m.value.inner.x"));
  }

  @Test
  void listOfListsOfStructs() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1, "ll", ListType.ofOptional(2, ListType.ofOptional(3, pointType(4, 5)))));

    assertPrunesTo(fileSchema, fileSchema.select("ll.element.element.x"));
  }

  @Test
  void listOfListsOfStructsInStruct() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2, "ll", ListType.ofOptional(3, ListType.ofOptional(4, pointType(5, 6)))),
                    NestedField.optional(7, "z", IntegerType.get()))));

    assertPrunesTo(fileSchema, fileSchema.select("s.ll.element.element.x"));
  }

  @Test
  void nestedStructInListOfLists() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "ll",
                ListType.ofOptional(
                    2,
                    ListType.ofOptional(
                        3,
                        StructType.of(
                            NestedField.optional(4, "inner", pointType(5, 6)),
                            NestedField.optional(7, "z", IntegerType.get()))))));

    assertPrunesTo(fileSchema, fileSchema.select("ll.element.element.inner.x"));
  }

  @Test
  void listOfStructsInListOfStructs() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "l",
                ListType.ofOptional(
                    2,
                    StructType.of(
                        NestedField.optional(3, "points", ListType.ofOptional(4, pointType(5, 6))),
                        NestedField.optional(7, "z", IntegerType.get())))));

    assertPrunesTo(fileSchema, fileSchema.select("l.element.points.element.x"));
  }

  @Test
  void twoLevelListOfStructsInStruct() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                            .addField(
                                Types.buildGroup(Type.Repetition.REPEATED)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                                            .id(5)
                                            .named("y"))
                                    .id(3)
                                    .named("array"))
                            .as(LogicalTypeAnnotation.listType())
                            .id(2)
                            .named("l"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                            .id(6)
                            .named("z"))
                    .id(1)
                    .named("s"))
            .named("table");

    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2,
                        "l",
                        ListType.ofRequired(
                            3, StructType.of(NestedField.optional(4, "x", IntegerType.get())))))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                            .addField(
                                Types.buildGroup(Type.Repetition.REPEATED)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                                            .id(4)
                                            .named("x"))
                                    .id(3)
                                    .named("array"))
                            .as(LogicalTypeAnnotation.listType())
                            .id(2)
                            .named("l"))
                    .id(1)
                    .named("s"))
            .named("table");

    assertThat(ParquetSchemaUtil.pruneColumns(fileSchema, projection)).isEqualTo(expected);
  }

  @Test
  void fieldsAtDifferentDepths() {
    Schema fileSchema =
        new Schema(
            NestedField.required(1, "id", IntegerType.get()),
            NestedField.optional(
                2,
                "s",
                StructType.of(
                    NestedField.optional(3, "inner", pointType(4, 5)),
                    NestedField.optional(6, "l", ListType.ofOptional(7, pointType(8, 9))),
                    NestedField.optional(
                        10, "m", MapType.ofOptional(11, 12, StringType.get(), pointType(13, 14))),
                    NestedField.optional(15, "z", IntegerType.get()))));

    assertPrunesTo(
        fileSchema, fileSchema.select("id", "s.inner.y", "s.l.element.x", "s.m.value.y", "s.z"));
  }

  @Test
  void fullNestedProjectionIsUnchanged() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(2, "inner", pointType(3, 4)),
                    NestedField.optional(
                        5, "ll", ListType.ofOptional(6, ListType.ofOptional(7, pointType(8, 9)))),
                    NestedField.optional(
                        10,
                        "m",
                        MapType.ofOptional(
                            11,
                            12,
                            StringType.get(),
                            ListType.ofOptional(13, pointType(14, 15)))))));

    assertPrunesTo(fileSchema, fileSchema);
  }

  @Test
  void variantInNestedStruct() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2,
                        "inner",
                        StructType.of(
                            NestedField.optional(3, "v", VariantType.get()),
                            NestedField.optional(4, "x", IntegerType.get()))),
                    NestedField.optional(5, "z", IntegerType.get()))));

    assertPrunesTo(fileSchema, fileSchema.select("s.inner.v"));
  }

  @Test
  void mapWithStructKeyInStruct() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2, "m", MapType.ofOptional(3, 4, pointType(5, 6), pointType(7, 8))),
                    NestedField.optional(9, "z", IntegerType.get()))));

    assertPrunesTo(fileSchema, fileSchema.select("s.m.value.x"));
  }

  @Test
  void projectionWithNestedFieldMissingFromFile() {
    Schema fileSchema =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(2, "inner", pointType(3, 4)),
                    NestedField.optional(5, "z", IntegerType.get()))));

    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2,
                        "inner",
                        StructType.of(
                            NestedField.optional(3, "x", IntegerType.get()),
                            NestedField.optional(6, "added", IntegerType.get()))))));

    MessageType pruned =
        ParquetSchemaUtil.pruneColumns(ParquetSchemaUtil.convert(fileSchema, "table"), projection);

    assertThat(pruned)
        .isEqualTo(ParquetSchemaUtil.convert(fileSchema.select("s.inner.x"), "table"));
  }

  @Test
  void nestedFieldsWithoutIds() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.OPTIONAL)
                            .addField(
                                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                                    .id(3)
                                    .named("x"))
                            .addField(
                                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                                    .named("y"))
                            .id(2)
                            .named("inner"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                            .named("z"))
                    .id(1)
                    .named("s"))
            .named("table");

    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "s",
                StructType.of(
                    NestedField.optional(
                        2,
                        "inner",
                        StructType.of(NestedField.optional(3, "x", IntegerType.get()))))));

    assertThat(ParquetSchemaUtil.pruneColumns(fileSchema, projection))
        .isEqualTo(ParquetSchemaUtil.convert(projection, "table"));
  }

  private static StructType pointType(int xId, int yId) {
    return StructType.of(
        NestedField.optional(xId, "x", IntegerType.get()),
        NestedField.optional(yId, "y", IntegerType.get()));
  }

  private static void assertPrunesTo(Schema fileSchema, Schema projection) {
    MessageType pruned =
        ParquetSchemaUtil.pruneColumns(ParquetSchemaUtil.convert(fileSchema, "table"), projection);

    assertThat(pruned).isEqualTo(ParquetSchemaUtil.convert(projection, "table"));
  }

  private static Type buildVariantType(int id, String name) {
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("metadata"))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("value"))
        .id(id)
        .named(name);
  }
}
