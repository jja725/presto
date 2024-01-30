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

import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.paimon.shade.guava30.com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Presto TransactionManager. */
public class PaimonTransactionManager {

    private final Map<ConnectorTransactionHandle, ConnectorMetadata> transactions =
            new ConcurrentHashMap<>();

    public ConnectorMetadata get(ConnectorTransactionHandle transaction) {
        ConnectorMetadata metadata = transactions.get(transaction);
        Preconditions.checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public ConnectorMetadata remove(ConnectorTransactionHandle transaction) {
        ConnectorMetadata metadata = transactions.remove(transaction);
        Preconditions.checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public void put(ConnectorTransactionHandle transaction, ConnectorMetadata metadata) {
        ConnectorMetadata existing = transactions.putIfAbsent(transaction, metadata);
        Preconditions.checkState(existing == null, "transaction already exists: %s", existing);
    }
}
