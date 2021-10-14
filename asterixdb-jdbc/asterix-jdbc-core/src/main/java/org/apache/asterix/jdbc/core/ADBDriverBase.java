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
import java.util.Enumeration;
import java.util.HashMap;
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

    private volatile ADBDriverContext context;

    public ADBDriverBase(String driverScheme, int defaultApiPort) {
        this.urlScheme = JDBC_SCHEME + Objects.requireNonNull(driverScheme);
        this.defaultApiPort = defaultApiPort;
    }

    protected static void registerDriver(java.sql.Driver driver) {
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            DriverManager.println(String.format("Error registering driver %s. %s", driver.getClass().getName(), e));
        }
    }

    protected static void parseConnectionProperties(Properties inProps, ADBDriverContext driverContext,
            Map<ADBDriverProperty, Object> outProperties, SQLWarning outWarning) throws SQLException {
        if (inProps != null) {
            for (Enumeration<?> en = inProps.propertyNames(); en.hasMoreElements();) {
                String name = en.nextElement().toString();
                String value = inProps.getProperty(name);
                parseConnectionProperty(name, value, driverContext, outProperties, outWarning);
            }
        }
    }

    protected static void parseConnectionProperty(String name, String textValue, ADBDriverContext driverContext,
            Map<ADBDriverProperty, Object> outProperties, SQLWarning outWarning) throws SQLException {
        ADBDriverProperty property = driverContext.getSupportedProperties().get(name);
        if (property == null) {
            outWarning.setNextWarning(
                    new SQLWarning(driverContext.getErrorReporter().warningParameterNotSupported(name)));
            return;
        }
        if (textValue == null || textValue.isEmpty()) {
            return;
        }
        Object value;
        try {
            value = Objects.requireNonNull(property.getValueParser().apply(textValue));
        } catch (RuntimeException e) {
            throw driverContext.getErrorReporter().errorParameterValueNotSupported(name);
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
            throw createErrorReporter().errorParameterValueNotSupported("URL");
        }
        String host = subUri.getHost();
        if (host == null) {
            throw createErrorReporter().errorParameterValueNotSupported("URL");
        }
        int port = subUri.getPort();
        if (port <= 0) {
            port = defaultApiPort;
        }

        Properties uriParams = getURIParameters(subUri);

        ADBDriverContext driverContext = getOrCreateDriverContext();
        SQLWarning warning = new SQLWarning();
        Map<ADBDriverProperty, Object> properties = new HashMap<>();
        parseConnectionProperties(uriParams, driverContext, properties, warning);
        parseConnectionProperties(info, driverContext, properties, warning);

        warning = warning.getNextWarning() != null ? warning.getNextWarning() : null;

        String path = subUri.getPath();
        String dataverseCanonicalName =
                path != null && path.length() > 1 && path.startsWith("/") ? path.substring(1) : null;

        ADBProtocolBase protocol = createProtocol(host, port, properties, driverContext);
        try {
            String databaseVersion = protocol.connect();
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

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        Collection<ADBDriverProperty> supportedProperties =
                getOrCreateDriverContext().getSupportedProperties().values();
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
        return getOrCreateDriverContext().getDriverVersion().getMajorVersion();
    }

    public int getMinorVersion() {
        return getOrCreateDriverContext().getDriverVersion().getMinorVersion();
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

    private ADBDriverContext getOrCreateDriverContext() {
        ADBDriverContext ctx = context;
        if (ctx == null) {
            synchronized (this) {
                ctx = context;
                if (ctx == null) {
                    context = ctx = createDriverContext();
                }
            }
        }
        return ctx;
    }

    protected ADBDriverContext createDriverContext() {
        return new ADBDriverContext(getDriverVersion(), getDriverSupportedProperties(), createErrorReporter(),
                getLogger());
    }

    protected ADBProductVersion getDriverVersion() {
        return ADBProductVersion.parseDriverVersion(getClass().getPackage());
    }

    protected Collection<ADBDriverProperty> getDriverSupportedProperties() {
        return Arrays.asList(ADBDriverProperty.Common.values());
    }

    protected ADBErrorReporter createErrorReporter() {
        return new ADBErrorReporter();
    }

    protected abstract ADBProtocolBase createProtocol(String host, int port, Map<ADBDriverProperty, Object> properties,
            ADBDriverContext driverContext) throws SQLException;

    protected ADBConnection createConnection(ADBProtocolBase protocol, String url, String databaseVersion,
            String dataverseCanonicalName, Map<ADBDriverProperty, Object> properties, SQLWarning connectWarning)
            throws SQLException {
        return new ADBConnection(protocol, url, databaseVersion, dataverseCanonicalName, properties, connectWarning);
    }

    protected abstract Properties getURIParameters(URI uri) throws SQLException;
}
