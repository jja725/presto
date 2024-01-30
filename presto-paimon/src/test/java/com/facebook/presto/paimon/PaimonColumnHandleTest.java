/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.paimon;

import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonColumnHandle}. */
public class PaimonColumnHandleTest {

    @Test
    public void testPrestoColumnHandle() {
        VarCharType varCharType = VarCharType.stringType(true);
        PaimonColumnHandle expected =
                new PaimonColumnHandle(
                        "name",
                        JsonSerdeUtil.toJson(varCharType),
                        createTestFunctionAndTypeManager().getType(parseTypeSignature(VARCHAR)));
        testRoundTrip(expected);
    }

    private void testRoundTrip(PaimonColumnHandle expected) {
        ObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(
                        Type.class, new TypeDeserializer(createTestFunctionAndTypeManager())));
        JsonCodec<PaimonColumnHandle> codec =
                new JsonCodecFactory(objectMapperProvider).jsonCodec(PaimonColumnHandle.class);
        String json = codec.toJson(expected);
        PaimonColumnHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getColumnName()).isEqualTo(expected.getColumnName());
        assertThat(actual.paimonType()).isEqualTo(expected.paimonType());
        assertThat(actual.getPrestoType()).isEqualTo(expected.getPrestoType());
        assertThat(actual.getTypeString()).isEqualTo(expected.getTypeString());
    }
}
