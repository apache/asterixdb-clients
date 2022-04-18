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

package org.apache.asterix.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.asterix.jdbc.Driver;
import org.apache.asterix.jdbc.core.ADBConnection;
import org.junit.Assert;

class JdbcConnectionTester extends JdbcTester {

    public void testGetConnectionViaDriverManager() throws SQLException {
        DriverManager.getConnection(testContext.getJdbcUrl()).close();
        DriverManager.getConnection(testContext.getJdbcUrl(), null).close();
        DriverManager.getConnection(testContext.getJdbcUrl(), new Properties()).close();
        DriverManager.getConnection(testContext.getJdbcUrl(), null, null).close();
    }

    public void testGetConnectionDirect() throws SQLException {
        Driver driver = new Driver();
        driver.connect(testContext.getJdbcUrl(), null).close();
        driver.connect(testContext.getJdbcUrl(), new Properties()).close();
    }

    public void testLifecycle() throws SQLException {
        Connection c = createConnection();
        Assert.assertNull(c.getWarnings());
        Assert.assertTrue(c.isValid( /*timeout in seconds*/ 30));
        Assert.assertFalse(c.isClosed());

        c.close();
        Assert.assertTrue(c.isClosed());

        // ok to call close() on a closed connection
        c.close();
        Assert.assertTrue(c.isClosed());

        // ok to call isValid() on a closed connection
        Assert.assertFalse(c.isValid(0));

        // errors on a closed connection
        assertErrorOnClosed(c, Connection::clearWarnings, "clearWarnings");
        assertErrorOnClosed(c, Connection::createStatement, "createStatement");
        assertErrorOnClosed(c, Connection::getAutoCommit, "getAutoCommit");
        assertErrorOnClosed(c, Connection::getCatalog, "getCatalog");
        assertErrorOnClosed(c, Connection::getClientInfo, "getClientInfo");
        assertErrorOnClosed(c, Connection::getHoldability, "getHoldability");
        assertErrorOnClosed(c, Connection::getMetaData, "getMetadata");
        assertErrorOnClosed(c, Connection::getSchema, "getSchema");
        assertErrorOnClosed(c, Connection::getTransactionIsolation, "getTransactionIsolation");
        assertErrorOnClosed(c, Connection::getWarnings, "getWarnings");
        assertErrorOnClosed(c, Connection::getTypeMap, "getTypeMap");
        assertErrorOnClosed(c, Connection::isReadOnly, "isReadOnly");
        assertErrorOnClosed(c, ci -> ci.prepareStatement("select 1"), "prepareStatement");
    }

    public void testCatalogSchema() throws SQLException {
        try (Connection c = createConnection()) {
            Assert.assertEquals(DEFAULT_DATAVERSE_NAME, c.getCatalog());
            Assert.assertNull(c.getSchema());
        }

        try (Connection c = createConnection(METADATA_DATAVERSE_NAME)) {
            Assert.assertEquals(METADATA_DATAVERSE_NAME, c.getCatalog());
            Assert.assertNull(c.getSchema());
        }

        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testCatalogSchema");
            String dvCanon = getCanonicalDataverseName(dataverse);
            String dataset = "ds1";
            s.execute(printCreateDataverse(dataverse));
            s.execute(printCreateDataset(dataverse, dataset));
            s.execute(printInsert(dataverse, dataset, dataGen("x", 1, 2, 3)));
            try (Connection c2 = createConnection(dvCanon); Statement s2 = c2.createStatement()) {
                Assert.assertEquals(dvCanon, c2.getCatalog());
                Assert.assertNull(c.getSchema());
                try (ResultSet rs2 =
                        s2.executeQuery(String.format("select count(*) from %s", printIdentifier(dataset)))) {
                    Assert.assertTrue(rs2.next());
                    Assert.assertEquals(3, rs2.getInt(1));
                }
            } finally {
                s.execute(printDropDataverse(dataverse));
            }
        }
    }

    // Connection.setReadOnly() hint is currently ignored
    // Connection.isReadOnly() always returns 'false'
    public void testReadOnlyMode() throws SQLException {
        try (Connection c = createConnection()) {
            Assert.assertFalse(c.isReadOnly());
            c.setReadOnly(true);
            Assert.assertFalse(c.isReadOnly());
        }
    }

    public void testWrapper() throws SQLException {
        try (Connection c = createConnection()) {
            Assert.assertTrue(c.isWrapperFor(ADBConnection.class));
            Assert.assertNotNull(c.unwrap(ADBConnection.class));
        }
    }
}
