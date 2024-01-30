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

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonSplit}. */
public class PaimonSplitTest {

    private final JsonCodec<PaimonSplit> codec = JsonCodec.jsonCodec(PaimonSplit.class);

    @Test
    public void testJsonRoundTrip() throws Exception {
        byte[] serializedTable = TestPrestoUtils.getSerializedTable();
        PaimonSplit expected = new PaimonSplit(Arrays.toString(serializedTable));
        String json = codec.toJson(expected);
        PaimonSplit actual = codec.fromJson(json);
        assertThat(actual.getSplitSerialized()).isEqualTo(expected.getSplitSerialized());
    }
}
