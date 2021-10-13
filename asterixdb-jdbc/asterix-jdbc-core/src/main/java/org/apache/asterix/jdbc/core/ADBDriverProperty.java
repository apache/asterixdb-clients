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

package org.apache.asterix.jdbc.core;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public interface ADBDriverProperty {

    String getPropertyName();

    Function<String, ?> getValueParser();

    Object getDefaultValue();

    boolean isHidden();

    enum Common implements ADBDriverProperty {

        USER("user", Function.identity(), null, false),
        PASSWORD("password", Function.identity(), null, false),
        CONNECT_TIMEOUT("connectTimeout", Integer::parseInt, null, false),
        SOCKET_TIMEOUT("socketTimeout", Integer::parseInt, null, false),
        MAX_WARNINGS("maxWarnings", Integer::parseInt, 10, false),
        CATALOG_DATAVERSE_MODE("catalogDataverseMode", Integer::parseInt, 1, false), // 1 -> CATALOG, 2 -> CATALOG_SCHEMA
        CATALOG_INCLUDES_SCHEMALESS("catalogIncludesSchemaless", Boolean::parseBoolean, false, false),
        SQL_COMPAT_MODE("sqlCompatMode", Boolean::parseBoolean, true, false), // Whether user statements are executed in 'SQL-compat' mode
        ACTIVE_REQUESTS_PATH("activeRequestsPath", Function.identity(), null, true),
        SSL("ssl", Boolean::parseBoolean, false, false);

        private final String propertyName;

        private final Function<String, ?> valueParser;

        private final Object defaultValue;

        private final boolean isHidden;

        Common(String propertyName, Function<String, ?> valueParser, Object defaultValue, boolean isHidden) {
            this.propertyName = Objects.requireNonNull(propertyName);
            this.valueParser = Objects.requireNonNull(valueParser);
            this.defaultValue = defaultValue;
            this.isHidden = isHidden;
        }

        @Override
        public String getPropertyName() {
            return propertyName;
        }

        @Override
        public Function<String, ?> getValueParser() {
            return valueParser;
        }

        @Override
        public Object getDefaultValue() {
            return defaultValue;
        }

        @Override
        public boolean isHidden() {
            return isHidden;
        }

        @Override
        public String toString() {
            return getPropertyName();
        }

        public Object fetchPropertyValue(Map<ADBDriverProperty, Object> properties) {
            return properties.getOrDefault(this, defaultValue);
        }
    }

    enum CatalogDataverseMode {
        CATALOG,
        CATALOG_SCHEMA;

        static CatalogDataverseMode valueOf(int n) {
            switch (n) {
                case 1:
                    return CATALOG;
                case 2:
                    return CATALOG_SCHEMA;
                default:
                    throw new IllegalArgumentException(String.valueOf(n));
            }
        }
    }
}
