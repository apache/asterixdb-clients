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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.jdbc.core.ADBPreparedStatement;
import org.junit.Assert;

class JdbcPreparedStatementTester extends JdbcTester {

    public void testLifecycle() throws SQLException {
        Connection c = createConnection();
        PreparedStatement s = c.prepareStatement(Q1);
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
        PreparedStatement s = c.prepareStatement(Q1);
        Assert.assertFalse(s.isClosed());
        c.close();
        Assert.assertTrue(s.isClosed());
    }

    public void testCloseOnCompletion() throws SQLException {
        try (Connection c = createConnection()) {
            PreparedStatement s = c.prepareStatement(Q1);
            Assert.assertFalse(s.isCloseOnCompletion());
            s.closeOnCompletion();
            Assert.assertTrue(s.isCloseOnCompletion());
            Assert.assertFalse(s.isClosed());
            ResultSet rs = s.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.next());
            rs.close();
            Assert.assertTrue(s.isClosed());
        }
    }

    public void testExecuteQuery() throws SQLException {
        try (Connection c = createConnection()) {
            // Query -> ok
            try (PreparedStatement s1 = c.prepareStatement(Q1); ResultSet rs1 = s1.executeQuery()) {
                Assert.assertTrue(rs1.next());
                Assert.assertEquals(1, rs1.getMetaData().getColumnCount());
                Assert.assertEquals(V1, rs1.getInt(1));
                Assert.assertFalse(rs1.next());
                Assert.assertFalse(rs1.isClosed());
            }

            // DDL -> error
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testExecuteQuery");
            try {
                PreparedStatement s2 = c.prepareStatement(printCreateDataverse(dataverse));
                s2.executeQuery();
                Assert.fail("DDL did not fail in executeQuery()");
            } catch (SQLException e) {
                String msg = e.getMessage();
                Assert.assertTrue(msg, msg.contains(ErrorCode.PROHIBITED_STATEMENT_CATEGORY.errorCode()));
            }

            // DML -> error
            String dataset = "ds1";
            PreparedStatement s3 = c.prepareStatement(printCreateDataverse(dataverse));
            s3.execute();
            PreparedStatement s4 = c.prepareStatement(printCreateDataset(dataverse, dataset));
            s4.execute();
            try {
                PreparedStatement s5 = c.prepareStatement(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
                s5.executeQuery();
                Assert.fail("DML did not fail in executeQuery()");
            } catch (SQLException e) {
                String msg = e.getMessage();
                Assert.assertTrue(msg, msg.contains(ErrorCode.PROHIBITED_STATEMENT_CATEGORY.errorCode()));
            }

            // Cleanup
            PreparedStatement s6 = c.prepareStatement(printDropDataverse(dataverse));
            s6.execute();
        }
    }

    public void testExecuteUpdate() throws SQLException {
        try (Connection c = createConnection()) {
            // DDL -> ok
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testExecuteUpdate");
            PreparedStatement s1 = c.prepareStatement(printCreateDataverse(dataverse));
            int res = s1.executeUpdate();
            Assert.assertEquals(0, res);
            String dataset = "ds1";
            PreparedStatement s2 = c.prepareStatement(printCreateDataset(dataverse, dataset));
            res = s2.executeUpdate();
            Assert.assertEquals(0, res);

            // DML -> ok
            PreparedStatement s3 = c.prepareStatement(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
            res = s3.executeUpdate();
            // currently, DML statements always return update count = 1
            Assert.assertEquals(1, res);

            // Query -> error
            try {
                PreparedStatement s4 = c.prepareStatement(Q1);
                s4.executeUpdate();
                Assert.fail("Query did not fail in executeUpdate()");
            } catch (SQLException e) {
                String msg = e.getMessage();
                Assert.assertTrue(msg, msg.contains("Invalid statement category"));
            }

            // Cleanup
            PreparedStatement s5 = c.prepareStatement(printDropDataverse(dataverse));
            s5.executeUpdate();
        }
    }

    public void testExecute() throws SQLException {
        try (Connection c = createConnection()) {
            // Query -> ok
            PreparedStatement s1 = c.prepareStatement(Q1);
            boolean res = s1.execute();
            Assert.assertTrue(res);
            Assert.assertEquals(-1, s1.getUpdateCount());
            try (ResultSet rs = s1.getResultSet()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(1, rs.getMetaData().getColumnCount());
                Assert.assertEquals(V1, rs.getInt(1));
                Assert.assertFalse(rs.next());
                Assert.assertFalse(rs.isClosed());
            }

            // DDL -> ok
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testExecute");
            PreparedStatement s2 = c.prepareStatement(printCreateDataverse(dataverse));
            res = s2.execute();
            Assert.assertFalse(res);
            Assert.assertEquals(0, s2.getUpdateCount());
            String dataset = "ds1";
            PreparedStatement s3 = c.prepareStatement(printCreateDataset(dataverse, dataset));
            res = s3.execute();
            Assert.assertFalse(res);

            // DML -> ok
            PreparedStatement s4 = c.prepareStatement(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
            res = s4.execute();
            Assert.assertFalse(res);
            // currently, DML statements always return update count = 1
            Assert.assertEquals(1, s4.getUpdateCount());

            // Cleanup
            PreparedStatement s5 = c.prepareStatement(printDropDataverse(dataverse));
            s5.execute();
        }
    }

    public void testGetResultSet() throws SQLException {
        try (Connection c = createConnection()) {
            // Query
            PreparedStatement s1 = c.prepareStatement(Q1);
            boolean res = s1.execute();
            Assert.assertTrue(res);
            ResultSet rs = s1.getResultSet();
            Assert.assertFalse(rs.isClosed());
            Assert.assertTrue(rs.next());
            Assert.assertFalse(s1.getMoreResults()); // closes current ResultSet
            Assert.assertTrue(rs.isClosed());

            PreparedStatement s2 = c.prepareStatement(Q1);
            res = s2.execute();
            Assert.assertTrue(res);
            rs = s2.getResultSet();
            Assert.assertFalse(rs.isClosed());
            Assert.assertTrue(rs.next());
            Assert.assertFalse(s2.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            Assert.assertFalse(rs.isClosed());
            rs.close();

            // DDL
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testGetResultSet");
            PreparedStatement s3 = c.prepareStatement(printCreateDataverse(dataverse));
            res = s3.execute();
            Assert.assertFalse(res);
            Assert.assertNull(s3.getResultSet());
            Assert.assertFalse(s3.getMoreResults());

            String dataset = "ds1";
            PreparedStatement s4 = c.prepareStatement(printCreateDataset(dataverse, dataset));
            res = s4.execute();
            Assert.assertFalse(res);

            // DML
            PreparedStatement s5 = c.prepareStatement(printInsert(dataverse, dataset, dataGen("x", 1, 2)));
            res = s5.execute();
            Assert.assertFalse(res);
            Assert.assertNull(s5.getResultSet());
            Assert.assertFalse(s5.getMoreResults());
        }
    }

    public void testMaxRows() throws SQLException {
        try (Connection c = createConnection()) {
            List<String> dataverse = Arrays.asList(getClass().getSimpleName(), "testMaxRows");
            String dataset = "ds1";
            String field = "x";
            PreparedStatement s1 = c.prepareStatement(printCreateDataverse(dataverse));
            s1.execute();
            PreparedStatement s2 = c.prepareStatement(printCreateDataset(dataverse, dataset));
            s2.execute();
            PreparedStatement s3 = c.prepareStatement(printInsert(dataverse, dataset, dataGen(field, 1, 2, 3)));
            s3.execute();

            PreparedStatement s4 = c.prepareStatement(String.format("select %s from %s.%s", field,
                    printDataverseName(dataverse), printIdentifier(dataset)));
            s4.setMaxRows(2);
            Assert.assertEquals(2, s4.getMaxRows());
            try (ResultSet rs = s4.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.next());
                Assert.assertFalse(rs.next());
            }
        }
    }

    public void testWarnings() throws SQLException {
        try (Connection c = createConnection();
                PreparedStatement s = c.prepareStatement("select double('x'), bigint('y')"); // --> NULL with warning
                ResultSet rs = s.executeQuery()) {
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
        try (Connection c = createConnection(); PreparedStatement s = c.prepareStatement(Q1)) {
            Assert.assertTrue(s.isWrapperFor(ADBPreparedStatement.class));
            Assert.assertNotNull(s.unwrap(ADBPreparedStatement.class));
        }
    }
}
