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

import static java.util.Objects.requireNonNull;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;

/** Presto {@link ColumnHandle}. */
public class PaimonColumnHandle implements ColumnHandle {

    private final String columnName;
    private final String typeString;
    private final Type prestoType;

    @JsonCreator
    public PaimonColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("typeString") String typeString,
            @JsonProperty("prestoType") Type prestoType) {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.typeString = requireNonNull(typeString, "typeString is null");
        this.prestoType = requireNonNull(prestoType, "columnType is null");
    }

    public static PaimonColumnHandle create(
            String columnName, DataType columnType, TypeManager typeManager) {
        return new PaimonColumnHandle(
                columnName,
                JsonSerdeUtil.toJson(columnType),
                PaimonTypeUtils.toPrestoType(columnType, typeManager));
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public String getTypeString() {
        return typeString;
    }

    public DataType paimonType() {
        return JsonSerdeUtil.fromJson(typeString, DataType.class);
    }

    @JsonProperty
    public Type getPrestoType() {
        return prestoType;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, prestoType);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        PaimonColumnHandle other = (PaimonColumnHandle) obj;
        return columnName.equals(other.columnName);
    }

    @Override
    public String toString() {
        return "{"
                + "columnName='"
                + columnName
                + '\''
                + ", typeString='"
                + typeString
                + '\''
                + ", prestoType="
                + prestoType
                + '}';
    }
}
