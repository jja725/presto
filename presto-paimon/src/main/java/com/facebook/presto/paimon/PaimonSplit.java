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

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.table.source.Split;

import java.util.Collections;
import java.util.List;

/** Presto {@link ConnectorSplit}. */
public class PaimonSplit implements ConnectorSplit {

    private final String splitSerialized;

    @JsonCreator
    public PaimonSplit(@JsonProperty("splitSerialized") String splitSerialized) {
        this.splitSerialized = splitSerialized;
    }

    public static PaimonSplit fromSplit(Split split) {
        return new PaimonSplit(EncodingUtils.encodeObjectToString(split));
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
        return ImmutableList.of();
    }

    public Split decodeSplit() {
        return EncodingUtils.decodeStringToObject(splitSerialized);
    }

    @JsonProperty
    public String getSplitSerialized() {
        return splitSerialized;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return NO_PREFERENCE;
    }

    @Override
    public Object getInfo() {
        return Collections.emptyMap();
    }
}
