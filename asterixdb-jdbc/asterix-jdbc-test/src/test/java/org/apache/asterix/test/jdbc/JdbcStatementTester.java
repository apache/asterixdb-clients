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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.jdbc.core.ADBStatement;
import org.junit.Assert;

class JdbcStatementTester extends JdbcTester {

    public void testLifecycle() throws SQLException {
        Connection c = createConnection();
        Statement s = c.createStatement();
        Assert.assertFalse(s.isClosed());
        Assert.assertSame(c, s.getConnection());

        s.close();
        Assert.assertTrue(s.isClosed());

        // ok to call close() on a closed statement
        s.close();
        Assert.assertTrue(s.isClosed());
    }

    public void testAutoCloseOnConnectionClose() throws SQLException {
        Connection c = createConnection();
        // check that a statement is automatically closed when the connection is closed
        Statement s = c.createStatement();
        Assert.assertFalse(s.isClosed());
        c.close();
        Assert.assertTrue(s.isClosed());
    }

    public void testCloseOnCompletion() throws SQLException {
        try (Connection c = createConnection()) {
            Statement s = c.createStatement();
            Assert.assertFalse(s.isCloseOnCompletion());
            s.closeOnCompletion();
            Assert.assertTrue(s.isCloseOnCompletion());
            Assert.assertFalse(s.isClosed());
            ResultSet rs = s.executeQuery(Q1);
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.next());
            rs.close();
            Assert.assertTrue(s.isClosed());
        }
    }

    public void testExecuteQuery() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            // Query -> ok
            try (ResultSet rs = s.executeQuery(Q1)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(1, rs.getMetaData().getColumnCount());
                Assert.assertEquals(V1, rs.getInt(1));
                Assert.assertFalse(rs.next());
                Assert.assertFalse(rs.isClosed());
            }

            // DDL -> error
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testExecuteQuery");
            try {
                s.executeQuery(printCreateDataverse(dataverse));
                Assert.fail("DDL did not fail in executeQuery()");
            } catch (SQLException e) {
                String msg = e.getMessage();
                Assert.assertTrue(msg, msg.contains(ErrorCode.PROHIBITED_STATEMENT_CATEGORY.errorCode()));
            }

            // DML -> error
            String dataset = "ds1";
            s.execute(printCreateDataverse(dataverse));
            s.execute(printCreateDataset(dataverse, dataset));
            try {
                s.executeQuery(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
                Assert.fail("DML did not fail in executeQuery()");
            } catch (SQLException e) {
                String msg = e.getMessage();
                Assert.assertTrue(msg, msg.contains(ErrorCode.PROHIBITED_STATEMENT_CATEGORY.errorCode()));
            }

            // Cleanup
            s.execute(printDropDataverse(dataverse));
        }
    }

    public void testExecuteUpdate() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            // DDL -> ok
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testExecuteUpdate");
            int res = s.executeUpdate(printCreateDataverse(dataverse));
            Assert.assertEquals(0, res);
            String dataset = "ds1";
            res = s.executeUpdate(printCreateDataset(dataverse, dataset));
            Assert.assertEquals(0, res);

            // DML -> ok
            res = s.executeUpdate(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
            // currently, DML statements always return update count = 1
            Assert.assertEquals(1, res);

            // Query -> error
            try {
                s.executeUpdate(Q1);
                Assert.fail("Query did not fail in executeUpdate()");
            } catch (SQLException e) {
                String msg = e.getMessage();
                Assert.assertTrue(msg, msg.contains("Invalid statement category"));
            }

            // Cleanup
            s.executeUpdate(printDropDataverse(dataverse));
        }
    }

    public void testExecute() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            // Query -> ok
            boolean res = s.execute(Q1);
            Assert.assertTrue(res);
            Assert.assertEquals(-1, s.getUpdateCount());
            try (ResultSet rs = s.getResultSet()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(1, rs.getMetaData().getColumnCount());
                Assert.assertEquals(V1, rs.getInt(1));
                Assert.assertFalse(rs.next());
                Assert.assertFalse(rs.isClosed());
            }

            // DDL -> ok
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testExecute");
            res = s.execute(printCreateDataverse(dataverse));
            Assert.assertFalse(res);
            Assert.assertEquals(0, s.getUpdateCount());
            String dataset = "ds1";
            res = s.execute(printCreateDataset(dataverse, dataset));
            Assert.assertFalse(res);

            // DML -> ok
            res = s.execute(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
            Assert.assertFalse(res);
            // currently, DML statements always return update count = 1
            Assert.assertEquals(1, s.getUpdateCount());

            // Cleanup
            s.execute(printDropDataverse(dataverse));
        }
    }

    public void testGetResultSet() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            // Query
            boolean res = s.execute(Q1);
            Assert.assertTrue(res);
            ResultSet rs = s.getResultSet();
            Assert.assertFalse(rs.isClosed());
            Assert.assertTrue(rs.next());
            Assert.assertFalse(s.getMoreResults()); // closes current ResultSet
            Assert.assertTrue(rs.isClosed());

            res = s.execute(Q1);
            Assert.assertTrue(res);
            rs = s.getResultSet();
            Assert.assertFalse(rs.isClosed());
            Assert.assertTrue(rs.next());
            Assert.assertFalse(s.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            Assert.assertFalse(rs.isClosed());
            rs.close();

            // DDL
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testGetResultSet");
            res = s.execute(printCreateDataverse(dataverse));
            Assert.assertFalse(res);
            Assert.assertNull(s.getResultSet());
            Assert.assertFalse(s.getMoreResults());

            String dataset = "ds1";
            res = s.execute(printCreateDataset(dataverse, dataset));
            Assert.assertFalse(res);

            // DML
            res = s.execute(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
            Assert.assertFalse(res);
            Assert.assertNull(s.getResultSet());
            Assert.assertFalse(s.getMoreResults());
        }
    }

    public void testMaxRows() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testMaxRows");
            String dataset = "ds1";
            String field = "x";
            s.execute(printCreateDataverse(dataverse));
            s.execute(printCreateDataset(dataverse, dataset));
            s.execute(printInsert(dataverse, dataset, dataGen(field, 1, 2, 3)));

            s.setMaxRows(2);
            Assert.assertEquals(2, s.getMaxRows());
            try (ResultSet rs = s.executeQuery(String.format("select %s from %s.%s", field,
                    printDataverseName(dataverse), printIdentifier(dataset)))) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.next());
                Assert.assertFalse(rs.next());
            }
        }
    }

    public void testWarnings() throws SQLException {
        try (Connection c = createConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("select double('x'), bigint('y')")) { // --> NULL with warning
            Assert.assertTrue(rs.next());
            rs.getDouble(1);
            Assert.assertTrue(rs.wasNull());
            rs.getLong(2);
            Assert.assertTrue(rs.wasNull());

            SQLWarning w = s.getWarnings();
            Assert.assertNotNull(w);
            String msg = w.getMessage();
            Assert.assertTrue(msg, msg.contains(ErrorCode.INVALID_FORMAT.errorCode()));

            SQLWarning w2 = w.getNextWarning();
            Assert.assertNotNull(w2);
            String msg2 = w.getMessage();
            Assert.assertTrue(msg2, msg2.contains(ErrorCode.INVALID_FORMAT.errorCode()));

            Assert.assertNull(w2.getNextWarning());
            s.clearWarnings();
            Assert.assertNull(s.getWarnings());
        }
    }

    public void testWrapper() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            Assert.assertTrue(s.isWrapperFor(ADBStatement.class));
            Assert.assertNotNull(s.unwrap(ADBStatement.class));
        }
    }
}
