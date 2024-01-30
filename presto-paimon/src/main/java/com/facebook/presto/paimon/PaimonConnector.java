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

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.EmptyConnectorCommitHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

/** Presto {@link Connector}. */
public class PaimonConnector implements Connector {

    private final PaimonTransactionManager transactionManager;
    private final PaimonSplitManager paimonSplitManager;
    private final PaimonPageSourceProvider paimonPageSourceProvider;
    private final PaimonMetadata paimonMetadata;

    @Inject
    public PaimonConnector(
            PaimonTransactionManager transactionManager,
            PaimonSplitManager paimonSplitManager,
            PaimonPageSourceProvider paimonPageSourceProvider,
            PaimonMetadata paimonMetadata) {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.paimonSplitManager = requireNonNull(paimonSplitManager, "paimonSplitManager is null");
        this.paimonPageSourceProvider =
                requireNonNull(paimonPageSourceProvider, "paimonPageSourceProvider is null");
        this.paimonMetadata = requireNonNull(paimonMetadata, "paimonMetadata is null");
    }

    @Override
    public ConnectorCommitHandle commit(ConnectorTransactionHandle transaction) {
        transactionManager.remove(transaction);
        return EmptyConnectorCommitHandle.INSTANCE;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        ConnectorTransactionHandle transaction = new PaimonTransactionHandle();
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(getClass().getClassLoader())) {
            transactionManager.put(transaction, paimonMetadata);
        }
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        ConnectorMetadata metadata = transactionManager.get(transactionHandle);
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return paimonSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return paimonPageSourceProvider;
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction) {
        transactionManager.remove(transaction);
    }
}
