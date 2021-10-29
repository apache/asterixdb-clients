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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class ADBDriverBase {

    static final int JDBC_MAJOR_VERSION = 4;

    static final int JDBC_MINOR_VERSION = 2;

    static final String JDBC_SCHEME = "jdbc:";

    static final String LOGGING_PROPERTY_SUFFIX = ".log.stderr";

    protected final String urlScheme;

    protected final int defaultApiPort;

    protected final ADBErrorReporter errorReporter;

    private volatile ADBProductVersion driverVersion;

    private volatile Map<String, ADBDriverProperty> supportedPropertiesIndex;

    private volatile ADBDriverContext context;

    public ADBDriverBase(String driverScheme, int defaultApiPort) {
        this.urlScheme = JDBC_SCHEME + Objects.requireNonNull(driverScheme);
        this.defaultApiPort = defaultApiPort;
        this.errorReporter = createErrorReporter();
    }

    protected static void registerDriver(java.sql.Driver driver) {
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            DriverManager.println(String.format("Error registering driver %s. %s", driver.getClass().getName(), e));
        }
    }

    protected void parseConnectionProperties(Properties inProps, Map<ADBDriverProperty, Object> outProperties,
            Map<String, ADBDriverProperty> supportedProperties, SQLWarning outWarning) throws SQLException {
        if (inProps != null) {
            for (Enumeration<?> en = inProps.propertyNames(); en.hasMoreElements();) {
                String name = en.nextElement().toString();
                String value = inProps.getProperty(name);
                parseConnectionProperty(name, value, supportedProperties, outProperties, outWarning);
            }
        }
    }

    protected void parseConnectionProperty(String name, String textValue,
            Map<String, ADBDriverProperty> supportedProperties, Map<ADBDriverProperty, Object> outProperties,
            SQLWarning outWarning) throws SQLException {
        ADBDriverProperty property = supportedProperties.get(name);
        if (property == null) {
            outWarning.setNextWarning(new SQLWarning(errorReporter.warningParameterNotSupported(name)));
            return;
        }
        if (textValue == null || textValue.isEmpty()) {
            return;
        }
        Object value;
        try {
            value = Objects.requireNonNull(property.getValueParser().apply(textValue));
        } catch (RuntimeException e) {
            throw errorReporter.errorParameterValueNotSupported(name);
        }
        outProperties.put(property, value);
    }

    private static Logger getParentLogger(Class<?> driverClass) {
        return Logger.getLogger(driverClass.getPackage().getName());
    }

    protected static void setupLogging(Class<? extends java.sql.Driver> driverClass) {
        String logLevel = System.getProperty(driverClass.getPackage().getName() + LOGGING_PROPERTY_SUFFIX);
        if (logLevel == null) {
            return;
        }
        Level level;
        try {
            level = Boolean.TRUE.toString().equals(logLevel) ? Level.ALL : Level.parse(logLevel.toUpperCase());
        } catch (IllegalArgumentException e) {
            // ignore
            return;
        }

        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(level);
        Logger parentLogger = getParentLogger(driverClass);
        parentLogger.setLevel(level);
        parentLogger.addHandler(ch);
    }

    public boolean acceptsURL(String url) {
        return url.startsWith(urlScheme);
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        URI subUri;
        try {
            subUri = new URI(url.substring(JDBC_SCHEME.length()));
        } catch (URISyntaxException e) {
            throw errorReporter.errorParameterValueNotSupported("URL");
        }
        String host = subUri.getHost();
        if (host == null) {
            throw errorReporter.errorParameterValueNotSupported("URL");
        }
        int port = subUri.getPort();
        if (port <= 0) {
            port = defaultApiPort;
        }

        Map<ADBDriverProperty, Object> properties = new HashMap<>();
        Map<String, ADBDriverProperty> supportedProperties = getOrCreateSupportedPropertiesIndex();
        SQLWarning warning = new SQLWarning();
        parseConnectionProperties(getURIParameters(subUri), properties, supportedProperties, warning);
        parseConnectionProperties(info, properties, supportedProperties, warning);
        warning = warning.getNextWarning() != null ? warning.getNextWarning() : null;

        checkDriverVersion(properties);

        String dataverseCanonicalName = getDataverseCanonicalNameFromURI(subUri);

        ADBDriverContext driverContext = getOrCreateDriverContext();
        ADBProtocolBase protocol = createProtocol(host, port, properties, driverContext);
        try {
            String serverVersion = protocol.connect();
            ADBProductVersion databaseVersion = protocol.parseDatabaseVersion(serverVersion);
            checkDatabaseVersion(properties, databaseVersion);
            return createConnection(protocol, url, databaseVersion, dataverseCanonicalName, properties, warning);
        } catch (SQLException e) {
            try {
                protocol.close();
            } catch (SQLException e2) {
                e.addSuppressed(e2);
            }
            throw e;
        }
    }

    protected String getDataverseCanonicalNameFromURI(URI subUri) {
        String path = subUri.getPath();
        return path != null && path.length() > 1 && path.startsWith("/") ? path.substring(1) : null;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        Collection<ADBDriverProperty> supportedProperties = getOrCreateSupportedPropertiesIndex().values();
        List<DriverPropertyInfo> result = new ArrayList<>(supportedProperties.size());
        for (ADBDriverProperty property : supportedProperties) {
            if (property.isHidden()) {
                continue;
            }
            Object defaultValue = property.getDefaultValue();
            DriverPropertyInfo propInfo = new DriverPropertyInfo(property.getPropertyName(),
                    defaultValue != null ? defaultValue.toString() : null);
            result.add(propInfo);
        }
        return result.toArray(new DriverPropertyInfo[0]);
    }

    public int getMajorVersion() {
        return getOrCreateDriverVersion().getMajorVersion();
    }

    public int getMinorVersion() {
        return getOrCreateDriverVersion().getMinorVersion();
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() {
        return getParentLogger(getClass());
    }

    protected Logger getLogger() {
        return Logger.getLogger(getClass().getName());
    }

    protected ADBDriverContext getOrCreateDriverContext() {
        ADBDriverContext result = context;
        if (result == null) {
            synchronized (this) {
                result = context;
                if (result == null) {
                    context = result = createDriverContext();
                }
            }
        }
        return result;
    }

    protected ADBDriverContext createDriverContext() {
        return new ADBDriverContext(getOrCreateDriverVersion(), errorReporter, getLogger());
    }

    protected ADBProductVersion getOrCreateDriverVersion() {
        ADBProductVersion result = driverVersion;
        if (result == null) {
            synchronized (this) {
                result = driverVersion;
                if (result == null) {
                    driverVersion = result = getDriverVersion();
                }
            }
        }
        return result;
    }

    protected abstract ADBProductVersion getDriverVersion();

    protected Collection<ADBDriverProperty> getSupportedProperties() {
        return Arrays.asList(ADBDriverProperty.Common.values());
    }

    private Map<String, ADBDriverProperty> getOrCreateSupportedPropertiesIndex() {
        Map<String, ADBDriverProperty> result = supportedPropertiesIndex;
        if (result == null) {
            synchronized (this) {
                result = supportedPropertiesIndex;
                if (result == null) {
                    supportedPropertiesIndex = result = createPropertyIndexByName(getSupportedProperties());
                }
            }
        }
        return result;
    }

    private static Map<String, ADBDriverProperty> createPropertyIndexByName(Collection<ADBDriverProperty> properties) {
        Map<String, ADBDriverProperty> m = new LinkedHashMap<>();
        for (ADBDriverProperty p : properties) {
            m.put(p.getPropertyName(), p);
        }
        return Collections.unmodifiableMap(m);
    }

    public void checkDriverVersion(Map<ADBDriverProperty, Object> properties) throws SQLException {
        ADBProductVersion minExpectedVersion =
                (ADBProductVersion) ADBDriverProperty.Common.MIN_DRIVER_VERSION.fetchPropertyValue(properties);
        if (minExpectedVersion != null) {
            ADBProductVersion driverVersion = getOrCreateDriverVersion();
            if (!driverVersion.isAtLeast(minExpectedVersion)) {
                throw errorReporter.errorUnexpectedDriverVersion(driverVersion, minExpectedVersion);
            }
        }
    }

    public void checkDatabaseVersion(Map<ADBDriverProperty, Object> properties, ADBProductVersion databaseVersion)
            throws SQLException {
        ADBProductVersion minExpectedVersion =
                (ADBProductVersion) ADBDriverProperty.Common.MIN_DATABASE_VERSION.fetchPropertyValue(properties);
        if (minExpectedVersion != null) {
            if (!databaseVersion.isAtLeast(minExpectedVersion)) {
                throw errorReporter.errorUnexpectedDatabaseVersion(databaseVersion, minExpectedVersion);
            }
        }
    }

    protected ADBErrorReporter createErrorReporter() {
        return new ADBErrorReporter();
    }

    protected abstract ADBProtocolBase createProtocol(String host, int port, Map<ADBDriverProperty, Object> properties,
            ADBDriverContext driverContext) throws SQLException;

    protected ADBConnection createConnection(ADBProtocolBase protocol, String url, ADBProductVersion databaseVersion,
            String dataverseCanonicalName, Map<ADBDriverProperty, Object> properties, SQLWarning connectWarning)
            throws SQLException {
        return new ADBConnection(protocol, url, databaseVersion, dataverseCanonicalName, properties, connectWarning);
    }

    protected abstract Properties getURIParameters(URI uri) throws SQLException;
}
