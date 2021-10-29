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

package org.apache.asterix.jdbc;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.asterix.jdbc.core.ADBDriverBase;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBErrorReporter;
import org.apache.asterix.jdbc.core.ADBProductVersion;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

public class Driver extends ADBDriverBase implements java.sql.Driver {

    private static final String DRIVER_SCHEME = "asterixdb:";

    private static final int DEFAULT_API_PORT = 19002;

    static {
        setupLogging(Driver.class);
        registerDriver(new Driver());
    }

    public Driver() {
        super(DRIVER_SCHEME, DEFAULT_API_PORT);
    }

    @Override
    protected ADBProtocol createProtocol(String host, int port, Map<ADBDriverProperty, Object> properties,
            ADBDriverContext driverContext) throws SQLException {
        return new ADBProtocol(host, port, properties, driverContext);
    }

    @Override
    protected Properties getURIParameters(URI uri) {
        List<NameValuePair> params = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);
        if (params.isEmpty()) {
            return null;
        }
        Properties properties = new Properties();
        for (NameValuePair pair : params) {
            properties.setProperty(pair.getName(), pair.getValue());
        }
        return properties;
    }

    @Override
    protected ADBErrorReporter createErrorReporter() {
        return new ADBErrorReporter() {
            @Override
            protected boolean isTimeoutConnectionError(IOException e) {
                return isInstanceOf(e, ADBProtocol.TIMEOUT_CONNECTION_ERRORS);
            }

            @Override
            protected boolean isTransientConnectionError(IOException e) {
                return isInstanceOf(e, ADBProtocol.TRANSIENT_CONNECTION_ERRORS);
            }
        };
    }

    @Override
    protected ADBProductVersion getDriverVersion() {
        Package driverPackage = getClass().getPackage();
        return parseDriverVersion(driverPackage.getImplementationTitle(), driverPackage.getImplementationVersion());
    }

    private static ADBProductVersion parseDriverVersion(String productName, String productVersion) {
        int majorVersion = 0, minorVersion = 0;
        if (productVersion != null) {
            String[] v = productVersion.split("\\.");
            try {
                majorVersion = Integer.parseInt(v[0]);
                if (v.length > 1) {
                    minorVersion = Integer.parseInt(v[1]);
                }
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return new ADBProductVersion(productName, productVersion, majorVersion, minorVersion);
    }
}
