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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Presto {@link ConnectorTableHandle}. */
public class PaimonTableHandle implements ConnectorTableHandle {

    private final String schemaName;
    private final String tableName;
    private final byte[] serializedTable;
    private final TupleDomain<PaimonColumnHandle> filter;
    private final Optional<List<ColumnHandle>> projectedColumns;

    private Table lazyTable;

    public PaimonTableHandle(String schemaName, String tableName, byte[] serializedTable) {
        this(schemaName, tableName, serializedTable, TupleDomain.all(), Optional.empty());
    }

    @JsonCreator
    public PaimonTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("serializedTable") byte[] serializedTable,
            @JsonProperty("filter") TupleDomain<PaimonColumnHandle> filter,
            @JsonProperty("projection") Optional<List<ColumnHandle>> projectedColumns) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.serializedTable = serializedTable;
        this.filter = filter;
        this.projectedColumns = projectedColumns;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public byte[] getSerializedTable() {
        return serializedTable;
    }

    @JsonProperty
    public TupleDomain<PaimonColumnHandle> getFilter() {
        return filter;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns() {
        return projectedColumns;
    }

    public PaimonTableHandle copy(TupleDomain<PaimonColumnHandle> filter) {
        return new PaimonTableHandle(
                schemaName, tableName, serializedTable, filter, projectedColumns);
    }

    public PaimonTableHandle copy(Optional<List<ColumnHandle>> projectedColumns) {
        return new PaimonTableHandle(
                schemaName, tableName, serializedTable, filter, projectedColumns);
    }

    public Table table() {
        if (lazyTable == null) {
            try {
                lazyTable =
                        InstantiationUtil.deserializeObject(
                                serializedTable, this.getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return lazyTable;
    }

    public ConnectorTableMetadata tableMetadata(TypeManager typeManager) {
        return new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName), columnMetadatas(typeManager));
    }

    public List<ColumnMetadata> columnMetadatas(TypeManager typeManager) {
        return table().rowType().getFields().stream()
                .map(
                        column ->
                                new ColumnMetadata(
                                        column.name(),
                                        Objects.requireNonNull(
                                                PaimonTypeUtils.toPrestoType(
                                                        column.type(), typeManager))))
                .collect(Collectors.toList());
    }

    public PaimonColumnHandle columnHandle(String field, TypeManager typeManager) {
        List<String> fieldNames = FieldNameUtils.fieldNames(table().rowType());
        int index = fieldNames.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format("Cannot find field %s in schema %s", field, fieldNames));
        }
        return PaimonColumnHandle.create(field, table().rowType().getTypeAt(index), typeManager);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonTableHandle that = (PaimonTableHandle) o;
        return Arrays.equals(serializedTable, that.serializedTable)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(filter, that.filter)
                && Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schemaName, tableName, filter, projectedColumns, Arrays.hashCode(serializedTable));
    }
}
