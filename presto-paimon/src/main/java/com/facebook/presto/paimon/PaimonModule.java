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

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.paimon.options.Options;

import java.util.Map;

/** Module for binding instance. */
public class PaimonModule implements Module {

    private final String connectorId;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final RowExpressionService rowExpressionService;
    private final Map<String, String> config;

    public PaimonModule(
            String connectorId,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            RowExpressionService rowExpressionService,
            Map<String, String> config) {
        this.connectorId = requireNonNull(connectorId, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
        this.rowExpressionService = rowExpressionService;
        this.config = config;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(PaimonConnectorId.class).toInstance(new PaimonConnectorId(connectorId));
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(PaimonConnector.class).in(Scopes.SINGLETON);
        binder.bind(PaimonMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PaimonSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PaimonPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(FunctionMetadataManager.class).toInstance(functionMetadataManager);
        binder.bind(StandardFunctionResolution.class).toInstance(standardFunctionResolution);
        binder.bind(RowExpressionService.class).toInstance(rowExpressionService);
        binder.bind(Options.class).toInstance(Options.fromMap(config));
        binder.bind(PaimonTransactionManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(PaimonConfig.class);
    }
}
