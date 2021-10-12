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

import java.sql.SQLException;
import java.util.Map;

import org.apache.asterix.jdbc.core.ADBDriverBase;
import org.apache.asterix.jdbc.core.ADBDriverContext;
import org.apache.asterix.jdbc.core.ADBDriverProperty;
import org.apache.asterix.jdbc.core.ADBProtocol;
import org.apache.asterix.jdbc.core.ADBProtocolBase;

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
    protected ADBProtocolBase createProtocol(String host, int port, Map<ADBDriverProperty, Object> properties,
            ADBDriverContext driverContext) throws SQLException {
        return new ADBProtocol(host, port, properties, driverContext);
    }
}
