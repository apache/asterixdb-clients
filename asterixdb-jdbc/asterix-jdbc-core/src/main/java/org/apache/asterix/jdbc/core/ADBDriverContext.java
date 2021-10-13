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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class ADBDriverContext {

    private final ADBProductVersion driverVersion;

    private final Map<String, ADBDriverProperty> supportedProperties;

    private final ADBErrorReporter errorReporter;

    private final Logger logger;

    private final ObjectReader genericObjectReader;

    private final ObjectWriter genericObjectWriter;

    private final ObjectReader admFormatObjectReader;

    private final ObjectWriter admFormatObjectWriter;

    public ADBDriverContext(ADBProductVersion driverVersion, Collection<ADBDriverProperty> driverSupportedProperties,
            ADBErrorReporter errorReporter, Logger logger) {
        this.driverVersion = Objects.requireNonNull(driverVersion);
        this.errorReporter = Objects.requireNonNull(errorReporter);
        this.logger = Objects.requireNonNull(logger);
        this.supportedProperties = createPropertyIndexByName(driverSupportedProperties);

        ObjectMapper genericObjectMapper = createGenericObjectMapper();
        this.genericObjectReader = genericObjectMapper.reader();
        this.genericObjectWriter = genericObjectMapper.writer();
        ObjectMapper admFormatObjectMapper = createADMFormatObjectMapper();
        this.admFormatObjectReader = admFormatObjectMapper.reader();
        this.admFormatObjectWriter = admFormatObjectMapper.writer();
    }

    protected ObjectMapper createGenericObjectMapper() {
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.NON_PRIVATE);
        // serialization
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // deserialization
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        om.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        return om;
    }

    protected ObjectMapper createADMFormatObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule serdeModule = new SimpleModule(getClass().getName());
        ADBStatement.configureADMFormatSerialization(serdeModule);
        ADBRowStore.configureADMFormatDeserialization(mapper, serdeModule);
        mapper.registerModule(serdeModule);
        return mapper;
    }

    private Map<String, ADBDriverProperty> createPropertyIndexByName(Collection<ADBDriverProperty> properties) {
        Map<String, ADBDriverProperty> m = new LinkedHashMap<>();
        for (ADBDriverProperty p : properties) {
            m.put(p.getPropertyName(), p);
        }
        return Collections.unmodifiableMap(m);
    }

    public ADBErrorReporter getErrorReporter() {
        return errorReporter;
    }

    public Logger getLogger() {
        return logger;
    }

    public ObjectReader getGenericObjectReader() {
        return genericObjectReader;
    }

    public ObjectWriter getGenericObjectWriter() {
        return genericObjectWriter;
    }

    public ObjectReader getAdmFormatObjectReader() {
        return admFormatObjectReader;
    }

    public ObjectWriter getAdmFormatObjectWriter() {
        return admFormatObjectWriter;
    }

    public ADBProductVersion getDriverVersion() {
        return driverVersion;
    }

    public Map<String, ADBDriverProperty> getSupportedProperties() {
        return supportedProperties;
    }
}
