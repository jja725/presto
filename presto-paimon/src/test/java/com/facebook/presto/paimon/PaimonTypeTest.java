/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.paimon;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.VarCharType;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonTypeUtils}. */
public class PaimonTypeTest {

    @Test
    public void testToPrestoType() {
        Type charType =
                PaimonTypeUtils.toPrestoType(DataTypes.CHAR(1), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(charType).getDisplayName()).isEqualTo("char(1)");

        Type varCharType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.VARCHAR(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(varCharType).getDisplayName()).isEqualTo("varchar");

        Type booleanType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.BOOLEAN(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(booleanType).getDisplayName()).isEqualTo("boolean");

        Type binaryType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.BINARY(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(binaryType).getDisplayName()).isEqualTo("varbinary");

        Type varBinaryType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.VARBINARY(10), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(varBinaryType).getDisplayName()).isEqualTo("varbinary");

        assertThat(
                        PaimonTypeUtils.toPrestoType(
                                        DataTypes.DECIMAL(38, 0),
                                        createTestFunctionAndTypeManager())
                                       .getDisplayName())
                .isEqualTo("decimal(38,0)");

        org.apache.paimon.types.DecimalType decimal = DataTypes.DECIMAL(2, 2);
        assertThat(
                        PaimonTypeUtils.toPrestoType(decimal, createTestFunctionAndTypeManager())
                                       .getDisplayName())
                .isEqualTo("decimal(2,2)");

        Type tinyIntType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.TINYINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(tinyIntType).getDisplayName()).isEqualTo("tinyint");

        Type smallIntType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.SMALLINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(smallIntType).getDisplayName()).isEqualTo("smallint");

        Type intType =
                PaimonTypeUtils.toPrestoType(DataTypes.INT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(intType).getDisplayName()).isEqualTo("integer");

        Type bigIntType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.BIGINT(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(bigIntType).getDisplayName()).isEqualTo("bigint");

        Type doubleType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.DOUBLE(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(doubleType).getDisplayName()).isEqualTo("double");

        Type dateType =
                PaimonTypeUtils.toPrestoType(DataTypes.DATE(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(dateType).getDisplayName()).isEqualTo("date");

        Type timeType =
                PaimonTypeUtils.toPrestoType(new TimeType(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(timeType).getDisplayName()).isEqualTo("time");

        Type timestampType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.TIMESTAMP(), createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(timestampType).getDisplayName()).isEqualTo("timestamp");

        Type localZonedTimestampType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                        createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(localZonedTimestampType).getDisplayName())
                .isEqualTo("timestamp with time zone");

        Type mapType =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()),
                        createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(mapType).getDisplayName())
                .isEqualTo("map(bigint, varchar)");

        Type row =
                PaimonTypeUtils.toPrestoType(
                        DataTypes.ROW(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "name", new VarCharType(Integer.MAX_VALUE))),
                        createTestFunctionAndTypeManager());
        assertThat(Objects.requireNonNull(row).getDisplayName())
                .isEqualTo("row(\"id\" integer, \"name\" varchar)");
    }

    @Test
    public void testToPaimonType() {
        DataType charType = PaimonTypeUtils.toPaimonType(CharType.createCharType(1));
        assertThat(charType.asSQLString()).isEqualTo("CHAR(1)");

        DataType varCharType =
                PaimonTypeUtils.toPaimonType(VarcharType.createUnboundedVarcharType());
        assertThat(varCharType.asSQLString()).isEqualTo("STRING");

        DataType booleanType = PaimonTypeUtils.toPaimonType(BooleanType.BOOLEAN);
        assertThat(booleanType.asSQLString()).isEqualTo("BOOLEAN");

        DataType varbinaryType = PaimonTypeUtils.toPaimonType(VarbinaryType.VARBINARY);
        assertThat(varbinaryType.asSQLString()).isEqualTo("BYTES");

        DataType decimalType = PaimonTypeUtils.toPaimonType(DecimalType.createDecimalType());
        assertThat(decimalType.asSQLString()).isEqualTo("DECIMAL(38, 0)");

        DataType tinyintType = PaimonTypeUtils.toPaimonType(TinyintType.TINYINT);
        assertThat(tinyintType.asSQLString()).isEqualTo("TINYINT");

        DataType smallintType = PaimonTypeUtils.toPaimonType(SmallintType.SMALLINT);
        assertThat(smallintType.asSQLString()).isEqualTo("SMALLINT");

        DataType intType = PaimonTypeUtils.toPaimonType(IntegerType.INTEGER);
        assertThat(intType.asSQLString()).isEqualTo("INT");

        DataType bigintType = PaimonTypeUtils.toPaimonType(BigintType.BIGINT);
        assertThat(bigintType.asSQLString()).isEqualTo("BIGINT");

        DataType floatType = PaimonTypeUtils.toPaimonType(RealType.REAL);
        assertThat(floatType.asSQLString()).isEqualTo("FLOAT");

        DataType doubleType = PaimonTypeUtils.toPaimonType(DoubleType.DOUBLE);
        assertThat(doubleType.asSQLString()).isEqualTo("DOUBLE");

        DataType dateType = PaimonTypeUtils.toPaimonType(DateType.DATE);
        assertThat(dateType.asSQLString()).isEqualTo("DATE");

        DataType timeType =
                PaimonTypeUtils.toPaimonType(com.facebook.presto.common.type.TimeType.TIME);
        assertThat(timeType.asSQLString()).isEqualTo("TIME(0)");

        DataType timestampType = PaimonTypeUtils.toPaimonType(TimestampType.TIMESTAMP);
        assertThat(timestampType.asSQLString()).isEqualTo("TIMESTAMP(6)");

        DataType timestampWithTimeZoneType =
                PaimonTypeUtils.toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat(timestampWithTimeZoneType.asSQLString())
                .isEqualTo("TIMESTAMP(6) WITH LOCAL TIME ZONE");

        DataType arrayType = PaimonTypeUtils.toPaimonType(new ArrayType(IntegerType.INTEGER));
        assertThat(arrayType.asSQLString()).isEqualTo("ARRAY<INT>");

        FunctionAndTypeManager testFunctionAndTypeManager = createTestFunctionAndTypeManager();
        Type parameterizedType =
                testFunctionAndTypeManager.getParameterizedType(
                        StandardTypes.MAP,
                        ImmutableList.of(
                                TypeSignatureParameter.of(BigintType.BIGINT.getTypeSignature()),
                                TypeSignatureParameter.of(
                                        VarcharType.createUnboundedVarcharType()
                                                .getTypeSignature())));
        DataType mapType = PaimonTypeUtils.toPaimonType(parameterizedType);
        assertThat(mapType.asSQLString()).isEqualTo("MAP<BIGINT, STRING>");

        List<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(java.util.Optional.of("id"), IntegerType.INTEGER));
        fields.add(
                new RowType.Field(
                        java.util.Optional.of("name"), VarcharType.createUnboundedVarcharType()));
        Type type = RowType.from(fields);
        DataType rowType = PaimonTypeUtils.toPaimonType(type);
        assertThat(rowType.asSQLString()).isEqualTo("ROW<`id` INT, `name` STRING>");
    }
}
