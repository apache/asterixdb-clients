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
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.asterix.jdbc.core.ADBDatabaseMetaData;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.junit.Assert;

class JdbcMetadataTester extends JdbcTester {

    static final String TABLE_CAT = "TABLE_CAT";
    static final String TABLE_CATALOG = "TABLE_CATALOG";
    static final String TABLE_SCHEM = "TABLE_SCHEM";
    static final String TABLE_NAME = "TABLE_NAME";
    static final String TABLE_TYPE = "TABLE_TYPE";
    static final String TABLE = "TABLE";
    static final String VIEW = "VIEW";
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String DATA_TYPE = "DATA_TYPE";
    static final String TYPE_NAME = "TYPE_NAME";
    static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    static final String NULLABLE = "NULLABLE";
    static final String KEY_SEQ = "KEY_SEQ";
    static final String PKTABLE_CAT = "PKTABLE_CAT";
    static final String PKTABLE_SCHEM = "PKTABLE_SCHEM";
    static final String PKTABLE_NAME = "PKTABLE_NAME";
    static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
    static final String FKTABLE_CAT = "FKTABLE_CAT";
    static final String FKTABLE_SCHEM = "FKTABLE_SCHEM";
    static final String FKTABLE_NAME = "FKTABLE_NAME";
    static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";

    static final String STRING = "string";
    static final String BIGINT = "int64";
    static final String DOUBLE = "double";

    static final List<String> DATASET_COLUMN_NAMES = Arrays.asList("tc", "ta", "tb");
    static final List<String> VIEW_COLUMN_NAMES = Arrays.asList("vb", "vc", "va");
    static final List<String> DATASET_COLUMN_TYPES = Arrays.asList(STRING, BIGINT, DOUBLE);
    static final List<SQLType> DATASET_COLUMN_JDBC_TYPES =
            Arrays.asList(JDBCType.VARCHAR, JDBCType.BIGINT, JDBCType.DOUBLE);
    static final int DATASET_PK_LEN = 2;

    public void testLifecycle() throws SQLException {
        Connection c = createConnection();
        Assert.assertSame(c, c.getMetaData().getConnection());
        c.close();
        try {
            c.getMetaData();
            Assert.fail("Got metadata on a closed connection");
        } catch (SQLException e) {
            Assert.assertEquals(SQL_STATE_CONNECTION_CLOSED, e.getSQLState());
        }
    }

    public void testProperties() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            Assert.assertEquals(testContext.getJdbcUrl(), md.getURL());
            Assert.assertNotNull(md.getDriverName());
            Assert.assertNotNull(md.getDriverVersion());
            Assert.assertNotNull(md.getDatabaseProductName());
            Assert.assertNotNull(md.getDatabaseProductVersion());
            Assert.assertEquals(4, md.getJDBCMajorVersion());
            Assert.assertEquals(2, md.getJDBCMinorVersion());
            Assert.assertTrue(md.isCatalogAtStart());
            Assert.assertEquals(".", md.getCatalogSeparator());
            Assert.assertEquals("`", md.getIdentifierQuoteString());
            Assert.assertTrue(md.allTablesAreSelectable());
            Assert.assertTrue(md.nullsAreSortedLow());
            Assert.assertFalse(md.nullsAreSortedHigh());
            Assert.assertFalse(md.nullsAreSortedAtStart());
            Assert.assertFalse(md.nullsAreSortedAtEnd());
            Assert.assertFalse(md.supportsCatalogsInTableDefinitions());
            Assert.assertFalse(md.supportsCatalogsInIndexDefinitions());
            Assert.assertFalse(md.supportsCatalogsInDataManipulation());
            Assert.assertFalse(md.supportsSchemasInTableDefinitions());
            Assert.assertFalse(md.supportsSchemasInIndexDefinitions());
            Assert.assertFalse(md.supportsSchemasInDataManipulation());
            Assert.assertTrue(md.supportsSubqueriesInComparisons());
            Assert.assertTrue(md.supportsSubqueriesInExists());
            Assert.assertTrue(md.supportsSubqueriesInIns());
            Assert.assertTrue(md.supportsCorrelatedSubqueries());
            Assert.assertTrue(md.supportsOrderByUnrelated());
            Assert.assertTrue(md.supportsExpressionsInOrderBy());
            Assert.assertTrue(md.supportsGroupBy());
            Assert.assertTrue(md.supportsGroupByUnrelated());
            Assert.assertTrue(md.supportsGroupByBeyondSelect());
            Assert.assertTrue(md.supportsOuterJoins());
            Assert.assertTrue(md.supportsMinimumSQLGrammar());
            Assert.assertTrue(md.supportsTableCorrelationNames());
            Assert.assertTrue(md.supportsUnionAll());
        }
    }

    public void testGetCatalogs() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getCatalogs()) {
                assertColumnValues(rs, TABLE_CAT, BUILT_IN_DATAVERSE_NAMES);
            }
            List<List<String>> newDataverseList = new ArrayList<>();
            try {
                createDataverses(s, newDataverseList);

                List<String> allCatalogs = new ArrayList<>(BUILT_IN_DATAVERSE_NAMES);
                for (List<String> n : newDataverseList) {
                    allCatalogs.add(getCanonicalDataverseName(n));
                }
                try (ResultSet rs = md.getCatalogs()) {
                    assertColumnValues(rs, TABLE_CAT, allCatalogs);
                }
            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    public void testGetCatalogsResultSetLifecycle() throws SQLException {
        // check that Connection.close() closes metadata ResultSet
        Connection c = createConnection();
        DatabaseMetaData md = c.getMetaData();
        ResultSet rs = md.getCatalogs();
        Assert.assertFalse(rs.isClosed());
        c.close();
        Assert.assertTrue(rs.isClosed());
    }

    public void testGetSchemas() throws SQLException {
        // get schemas in the default dataverse
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getSchemas()) {
                assertColumnValues(rs, Arrays.asList(TABLE_SCHEM, TABLE_CATALOG), Arrays
                        .asList(Collections.singletonList(null), Collections.singletonList(DEFAULT_DATAVERSE_NAME)));
            }
        }

        // get schemas in the connection's dataverse
        try (Connection c = createConnection(METADATA_DATAVERSE_NAME)) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getSchemas()) {
                assertColumnValues(rs, Arrays.asList(TABLE_SCHEM, TABLE_CATALOG), Arrays
                        .asList(Collections.singletonList(null), Collections.singletonList(METADATA_DATAVERSE_NAME)));
            }
        }

        // get schemas in the connection's dataverse #2
        try (Connection c = createConnection()) {
            c.setCatalog(METADATA_DATAVERSE_NAME);
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getSchemas()) {
                assertColumnValues(rs, Arrays.asList(TABLE_SCHEM, TABLE_CATALOG), Arrays
                        .asList(Collections.singletonList(null), Collections.singletonList(METADATA_DATAVERSE_NAME)));
            }
        }

        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            // we don't have any schemas without catalogs
            try (ResultSet rs = md.getSchemas("", null)) {
                Assert.assertEquals(0, countRows(rs));
            }
        }

        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<List<String>> newDataverseList = new ArrayList<>();
            try {
                createDataverses(s, newDataverseList);

                List<String> allCatalogs = new ArrayList<>(BUILT_IN_DATAVERSE_NAMES);
                for (List<String> n : newDataverseList) {
                    allCatalogs.add(getCanonicalDataverseName(n));
                }
                DatabaseMetaData md = c.getMetaData();
                try (ResultSet rs = md.getSchemas("", null)) {
                    Assert.assertFalse(rs.next());
                }
                try (ResultSet rs = md.getSchemas(null, null)) {
                    assertColumnValues(rs, Arrays.asList(TABLE_SCHEM, TABLE_CATALOG),
                            Arrays.asList(Collections.nCopies(allCatalogs.size(), null), allCatalogs));
                }
                try (ResultSet rs = md.getSchemas("x", null)) {
                    assertColumnValues(rs, Arrays.asList(TABLE_SCHEM, TABLE_CATALOG),
                            Arrays.asList(Collections.singletonList(null), Collections.singletonList("x")));
                }
            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    public void testGetTableTypes() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getTableTypes()) {
                assertColumnValues(rs, TABLE_TYPE, Arrays.asList(TABLE, VIEW));
            }
        }
    }

    public void testGetTables() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getTables(METADATA_DATAVERSE_NAME, null, null, null)) {
                int n = countRows(rs);
                Assert.assertTrue(String.valueOf(n), n > 10);
            }
            try (ResultSet rs = md.getTables(METADATA_DATAVERSE_NAME, null, "Data%", null)) {
                int n = countRows(rs);
                Assert.assertTrue(String.valueOf(n), n > 2);
            }
            // we don't have any tables without catalogs
            try (ResultSet rs = md.getTables("", null, null, null)) {
                int n = countRows(rs);
                Assert.assertEquals(0, n);
            }
        }

        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<List<String>> newDataverseList = new ArrayList<>();
            List<Pair<List<String>, String>> newDatasetList = new ArrayList<>();
            List<Pair<List<String>, String>> newViewList = new ArrayList<>();

            try {
                createDataversesDatasetsViews(s, newDataverseList, newDatasetList, newViewList);

                DatabaseMetaData md = c.getMetaData();
                List<String> expectedColumns = Arrays.asList(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE);
                List<String> expectedTableCat = new ArrayList<>();
                List<String> expectedTableSchem = new ArrayList<>();
                List<String> expectedTableName = new ArrayList<>();
                List<String> expectedTableType = new ArrayList<>();

                // Test getTables() in all catalogs
                for (Pair<List<String>, String> p : newDatasetList) {
                    expectedTableCat.add(getCanonicalDataverseName(p.first));
                    expectedTableSchem.add(null);
                    expectedTableName.add(p.second);
                    expectedTableType.add(TABLE);
                }
                // using table name pattern
                try (ResultSet rs = md.getTables(null, null, "t%", null)) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedTableType));
                }
                // using table type
                try (ResultSet rs = md.getTables(null, null, null, new String[] { TABLE })) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedTableType),
                            JdbcMetadataTester::isMetadataCatalog);
                }
                // all tables
                for (Pair<List<String>, String> p : newViewList) {
                    expectedTableCat.add(getCanonicalDataverseName(p.first));
                    expectedTableSchem.add(null);
                    expectedTableName.add(p.second);
                    expectedTableType.add(VIEW);
                }
                try (ResultSet rs = md.getTables(null, null, null, null)) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedTableType),
                            JdbcMetadataTester::isMetadataCatalog);
                }
                try (ResultSet rs = md.getTables(null, "", null, null)) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedTableType),
                            JdbcMetadataTester::isMetadataCatalog);
                }
                try (ResultSet rs = md.getTables(null, null, null, new String[] { TABLE, VIEW })) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedTableType),
                            JdbcMetadataTester::isMetadataCatalog);
                }

                // Test getTables() in a particular catalog
                for (List<String> dvi : newDataverseList) {
                    expectedTableCat.clear();
                    expectedTableSchem.clear();
                    expectedTableName.clear();
                    expectedTableType.clear();
                    String dvic = getCanonicalDataverseName(dvi);
                    for (Pair<List<String>, String> p : newDatasetList) {
                        String dv = getCanonicalDataverseName(p.first);
                        if (dv.equals(dvic)) {
                            expectedTableCat.add(dv);
                            expectedTableSchem.add(null);
                            expectedTableName.add(p.second);
                            expectedTableType.add(TABLE);
                        }
                    }
                    // using table name pattern
                    try (ResultSet rs = md.getTables(dvic, null, "t%", null)) {
                        assertColumnValues(rs, expectedColumns, Arrays.asList(expectedTableCat, expectedTableSchem,
                                expectedTableName, expectedTableType));
                    }
                    // using table type
                    try (ResultSet rs = md.getTables(dvic, null, null, new String[] { TABLE })) {
                        assertColumnValues(rs, expectedColumns, Arrays.asList(expectedTableCat, expectedTableSchem,
                                expectedTableName, expectedTableType));
                    }
                    for (Pair<List<String>, String> p : newViewList) {
                        String dv = getCanonicalDataverseName(p.first);
                        if (dv.equals(dvic)) {
                            expectedTableCat.add(dv);
                            expectedTableSchem.add(null);
                            expectedTableName.add(p.second);
                            expectedTableType.add(VIEW);
                        }
                    }
                    try (ResultSet rs = md.getTables(dvic, null, null, null)) {
                        assertColumnValues(rs, expectedColumns, Arrays.asList(expectedTableCat, expectedTableSchem,
                                expectedTableName, expectedTableType));
                    }
                    try (ResultSet rs = md.getTables(dvic, "", null, null)) {
                        assertColumnValues(rs, expectedColumns, Arrays.asList(expectedTableCat, expectedTableSchem,
                                expectedTableName, expectedTableType));
                    }
                    try (ResultSet rs = md.getTables(dvic, null, null, new String[] { TABLE, VIEW })) {
                        assertColumnValues(rs, expectedColumns, Arrays.asList(expectedTableCat, expectedTableSchem,
                                expectedTableName, expectedTableType));
                    }
                }

                // non-existent catalog
                try (ResultSet rs = md.getTables("UNKNOWN", null, null, null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent schema
                try (ResultSet rs = md.getTables(null, "UNKNOWN", null, null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent table name
                try (ResultSet rs = md.getTables(null, null, "UNKNOWN", null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent table type
                try (ResultSet rs = md.getTables(null, null, null, new String[] { "UNKNOWN" })) {
                    Assert.assertEquals(0, countRows(rs));
                }

            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    public void testGetColumns() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getColumns(METADATA_DATAVERSE_NAME, null, null, null)) {
                int n = countRows(rs);
                Assert.assertTrue(String.valueOf(n), n > 50);
            }
            try (ResultSet rs = md.getColumns(METADATA_DATAVERSE_NAME, null, "Data%", null)) {
                int n = countRows(rs);
                Assert.assertTrue(String.valueOf(n), n > 20);
            }
            // we don't have any columns without catalogs
            try (ResultSet rs = md.getColumns("", null, null, null)) {
                int n = countRows(rs);
                Assert.assertEquals(0, n);
            }
        }

        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<List<String>> newDataverseList = new ArrayList<>();
            List<Pair<List<String>, String>> newDatasetList = new ArrayList<>();
            List<Pair<List<String>, String>> newViewList = new ArrayList<>();

            try {
                createDataversesDatasetsViews(s, newDataverseList, newDatasetList, newViewList);

                DatabaseMetaData md = c.getMetaData();

                List<String> expectedColumns = Arrays.asList(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                        TYPE_NAME, ORDINAL_POSITION, NULLABLE);
                List<String> expectedTableCat = new ArrayList<>();
                List<String> expectedTableSchem = new ArrayList<>();
                List<String> expectedTableName = new ArrayList<>();
                List<String> expectedColumnName = new ArrayList<>();
                List<Integer> expectedDataType = new ArrayList<>();
                List<String> expectedTypeName = new ArrayList<>();
                List<Integer> expectedOrdinalPosition = new ArrayList<>();
                List<Integer> expectedNullable = new ArrayList<>();

                // Test getColumns() in all catalogs

                // datasets only
                for (Pair<List<String>, String> p : newDatasetList) {
                    for (int i = 0, n = DATASET_COLUMN_NAMES.size(); i < n; i++) {
                        String columnName = DATASET_COLUMN_NAMES.get(i);
                        String columnType = DATASET_COLUMN_TYPES.get(i);
                        SQLType columnJdbcType = DATASET_COLUMN_JDBC_TYPES.get(i);
                        expectedTableCat.add(getCanonicalDataverseName(p.first));
                        expectedTableSchem.add(null);
                        expectedTableName.add(p.second);
                        expectedColumnName.add(columnName);
                        expectedDataType.add(columnJdbcType.getVendorTypeNumber());
                        expectedTypeName.add(columnType);
                        expectedOrdinalPosition.add(i + 1);
                        expectedNullable.add(
                                i < DATASET_PK_LEN ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable);
                    }
                }
                // using column name pattern
                try (ResultSet rs = md.getColumns(null, null, null, "t%")) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedColumnName,
                                    expectedDataType, expectedTypeName, expectedOrdinalPosition, expectedNullable));
                }
                // using table name pattern
                try (ResultSet rs = md.getColumns(null, null, "t%", null)) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedColumnName,
                                    expectedDataType, expectedTypeName, expectedOrdinalPosition, expectedNullable));
                }
                // all columns
                expectedTableCat.clear();
                expectedTableSchem.clear();
                expectedTableName.clear();
                expectedColumnName.clear();
                expectedDataType.clear();
                expectedTypeName.clear();
                expectedOrdinalPosition.clear();
                expectedNullable.clear();

                int dsIdx = 0, vIdx = 0;
                for (List<String> dvName : newDataverseList) {
                    String dvNameCanonical = getCanonicalDataverseName(dvName);
                    for (; dsIdx < newDatasetList.size() && newDatasetList.get(dsIdx).first.equals(dvName); dsIdx++) {
                        String dsName = newDatasetList.get(dsIdx).second;
                        addExpectedColumnNamesForGetColumns(dvNameCanonical, dsName, DATASET_COLUMN_NAMES,
                                expectedTableCat, expectedTableSchem, expectedTableName, expectedColumnName,
                                expectedDataType, expectedTypeName, expectedOrdinalPosition, expectedNullable);
                    }
                    for (; vIdx < newViewList.size() && newViewList.get(vIdx).first.equals(dvName); vIdx++) {
                        String vName = newViewList.get(vIdx).second;
                        addExpectedColumnNamesForGetColumns(dvNameCanonical, vName, VIEW_COLUMN_NAMES, expectedTableCat,
                                expectedTableSchem, expectedTableName, expectedColumnName, expectedDataType,
                                expectedTypeName, expectedOrdinalPosition, expectedNullable);
                    }
                }

                try (ResultSet rs = md.getColumns(null, null, null, null)) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedColumnName,
                                    expectedDataType, expectedTypeName, expectedOrdinalPosition, expectedNullable),
                            JdbcMetadataTester::isMetadataCatalog);
                }
                try (ResultSet rs = md.getColumns(null, "", null, null)) {
                    assertColumnValues(rs, expectedColumns,
                            Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName, expectedColumnName,
                                    expectedDataType, expectedTypeName, expectedOrdinalPosition, expectedNullable),
                            JdbcMetadataTester::isMetadataCatalog);
                }

                // Test getColumns() in a particular catalog
                for (List<String> dvName : newDataverseList) {
                    expectedTableCat.clear();
                    expectedTableSchem.clear();
                    expectedTableName.clear();
                    expectedColumnName.clear();
                    expectedDataType.clear();
                    expectedTypeName.clear();
                    expectedOrdinalPosition.clear();
                    expectedNullable.clear();

                    String dvNameCanonical = getCanonicalDataverseName(dvName);
                    for (Pair<List<String>, String> p : newDatasetList) {
                        if (dvName.equals(p.first)) {
                            addExpectedColumnNamesForGetColumns(dvNameCanonical, p.second, DATASET_COLUMN_NAMES,
                                    expectedTableCat, expectedTableSchem, expectedTableName, expectedColumnName,
                                    expectedDataType, expectedTypeName, expectedOrdinalPosition, expectedNullable);
                        }
                    }
                    try (ResultSet rs = md.getColumns(dvNameCanonical, null, "t%", null)) {
                        assertColumnValues(rs, expectedColumns,
                                Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName,
                                        expectedColumnName, expectedDataType, expectedTypeName, expectedOrdinalPosition,
                                        expectedNullable),
                                JdbcMetadataTester::isMetadataCatalog);
                    }
                    try (ResultSet rs = md.getColumns(dvNameCanonical, null, null, "t%")) {
                        assertColumnValues(rs, expectedColumns,
                                Arrays.asList(expectedTableCat, expectedTableSchem, expectedTableName,
                                        expectedColumnName, expectedDataType, expectedTypeName, expectedOrdinalPosition,
                                        expectedNullable),
                                JdbcMetadataTester::isMetadataCatalog);
                    }
                }

                // non-existent catalog
                try (ResultSet rs = md.getColumns("UNKNOWN", null, null, null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent schema
                try (ResultSet rs = md.getColumns(null, "UNKNOWN", null, null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent table name
                try (ResultSet rs = md.getColumns(null, null, "UNKNOWN", null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent column names
                try (ResultSet rs = md.getColumns(null, null, null, "UNKNOWN")) {
                    Assert.assertEquals(0, countRows(rs));
                }

            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    private void addExpectedColumnNamesForGetColumns(String dvNameCanonical, String dsName, List<String> columnNames,
            List<String> outTableCat, List<String> outTableSchem, List<String> outTableName, List<String> outColumnName,
            List<Integer> outDataType, List<String> outTypeName, List<Integer> outOrdinalPosition,
            List<Integer> outNullable) {
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            String columnType = DATASET_COLUMN_TYPES.get(i);
            SQLType columnJdbcType = DATASET_COLUMN_JDBC_TYPES.get(i);
            outTableCat.add(dvNameCanonical);
            outTableSchem.add(null);
            outTableName.add(dsName);
            outColumnName.add(columnName);
            outDataType.add(columnJdbcType.getVendorTypeNumber());
            outTypeName.add(columnType);
            outOrdinalPosition.add(i + 1);
            outNullable.add(i < JdbcMetadataTester.DATASET_PK_LEN ? DatabaseMetaData.columnNoNulls
                    : DatabaseMetaData.columnNullable);
        }
    }

    public void testGetPrimaryKeys() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getPrimaryKeys(METADATA_DATAVERSE_NAME, null, null)) {
                int n = countRows(rs);
                Assert.assertTrue(String.valueOf(n), n > 20);
            }
            try (ResultSet rs = md.getPrimaryKeys(METADATA_DATAVERSE_NAME, null, "Data%")) {
                int n = countRows(rs);
                Assert.assertTrue(String.valueOf(n), n > 4);
            }
            // we don't have any tables without catalogs
            try (ResultSet rs = md.getPrimaryKeys("", null, null)) {
                int n = countRows(rs);
                Assert.assertEquals(0, n);
            }
        }

        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<List<String>> newDataverseList = new ArrayList<>();
            List<Pair<List<String>, String>> newDatasetList = new ArrayList<>();
            List<Pair<List<String>, String>> newViewList = new ArrayList<>();

            try {
                createDataversesDatasetsViews(s, newDataverseList, newDatasetList, newViewList);

                DatabaseMetaData md = c.getMetaData();

                List<String> expectedColumns = Arrays.asList(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ);
                List<String> expectedTableCat = new ArrayList<>();
                List<String> expectedTableSchem = new ArrayList<>();
                List<String> expectedTableName = new ArrayList<>();
                List<String> expectedColumnName = new ArrayList<>();
                List<Integer> expectedKeySeq = new ArrayList<>();

                // Test getPrimaryKeys() for a particular dataset/view
                for (int i = 0, n = newDatasetList.size(); i < n; i++) {
                    for (int j = 0; j < 2; j++) {
                        Pair<List<String>, String> p = j == 0 ? newDatasetList.get(i) : newViewList.get(i);
                        List<String> columnNames = j == 0 ? DATASET_COLUMN_NAMES : VIEW_COLUMN_NAMES;
                        String dvNameCanonical = getCanonicalDataverseName(p.first);
                        String dsName = p.second;

                        expectedTableCat.clear();
                        expectedTableSchem.clear();
                        expectedTableName.clear();
                        expectedColumnName.clear();
                        expectedKeySeq.clear();

                        List<String> pkColumnNames = columnNames.subList(0, DATASET_PK_LEN);
                        addExpectedColumnNamesForGetPrimaryKeys(dvNameCanonical, dsName, pkColumnNames,
                                expectedTableCat, expectedTableSchem, expectedTableName, expectedColumnName,
                                expectedKeySeq);

                        try (ResultSet rs = md.getPrimaryKeys(dvNameCanonical, null, dsName)) {
                            assertColumnValues(rs, expectedColumns, Arrays.asList(expectedTableCat, expectedTableSchem,
                                    expectedTableName, expectedColumnName, expectedKeySeq));
                        }
                    }
                }

                // non-existent catalog
                try (ResultSet rs = md.getPrimaryKeys("UNKNOWN", null, null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent schema
                try (ResultSet rs = md.getPrimaryKeys(null, "UNKNOWN", null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent table name
                try (ResultSet rs = md.getPrimaryKeys(null, null, "UNKNOWN")) {
                    Assert.assertEquals(0, countRows(rs));
                }

            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    private void addExpectedColumnNamesForGetPrimaryKeys(String dvNameCanonical, String dsName,
            List<String> pkColumnNames, List<String> outTableCat, List<String> outTableSchem, List<String> outTableName,
            List<String> outColumnName, List<Integer> outKeySeq) {
        List<String> pkColumnNamesSorted = new ArrayList<>(pkColumnNames);
        Collections.sort(pkColumnNamesSorted);
        for (int i = 0; i < pkColumnNames.size(); i++) {
            String pkColumnName = pkColumnNamesSorted.get(i);
            outTableCat.add(dvNameCanonical);
            outTableSchem.add(null);
            outTableName.add(dsName);
            outColumnName.add(pkColumnName);
            outKeySeq.add(pkColumnNames.indexOf(pkColumnName) + 1);
        }
    }

    public void testGetImportedKeys() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<List<String>> newDataverseList = new ArrayList<>();
            List<Pair<List<String>, String>> newDatasetList = new ArrayList<>();
            List<Pair<List<String>, String>> newViewList = new ArrayList<>();

            try {
                createDataversesDatasetsViews(s, newDataverseList, newDatasetList, newViewList);

                DatabaseMetaData md = c.getMetaData();

                List<String> expectedColumns = Arrays.asList(PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME,
                        FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ);
                List<String> expectedPKTableCat = new ArrayList<>();
                List<String> expectedPKTableSchem = new ArrayList<>();
                List<String> expectedPKTableName = new ArrayList<>();
                List<String> expectedPKColumnName = new ArrayList<>();
                List<String> expectedFKTableCat = new ArrayList<>();
                List<String> expectedFKTableSchem = new ArrayList<>();
                List<String> expectedFKTableName = new ArrayList<>();
                List<String> expectedFKColumnName = new ArrayList<>();
                List<Integer> expectedKeySeq = new ArrayList<>();

                // Test getImportedKeys() for a particular view
                for (int i = 0, n = newViewList.size(); i < n; i++) {
                    Pair<List<String>, String> p = newViewList.get(i);
                    List<String> dvName = p.first;
                    String dvNameCanonical = getCanonicalDataverseName(dvName);
                    String viewName = p.second;

                    expectedPKTableCat.clear();
                    expectedPKTableSchem.clear();
                    expectedPKTableName.clear();
                    expectedPKColumnName.clear();
                    expectedFKTableCat.clear();
                    expectedFKTableSchem.clear();
                    expectedFKTableName.clear();
                    expectedFKColumnName.clear();

                    expectedKeySeq.clear();

                    List<String> pkFkColumnNames = VIEW_COLUMN_NAMES.subList(0, DATASET_PK_LEN);
                    List<String> fkRefs = IntStream.range(0, i).mapToObj(newViewList::get)
                            .filter(p2 -> p2.first.equals(dvName)).map(p2 -> p2.second).collect(Collectors.toList());

                    addExpectedColumnNamesForGetImportedKeys(dvNameCanonical, viewName, pkFkColumnNames, fkRefs,
                            expectedPKTableCat, expectedPKTableSchem, expectedPKTableName, expectedPKColumnName,
                            expectedFKTableCat, expectedFKTableSchem, expectedFKTableName, expectedFKColumnName,
                            expectedKeySeq);

                    try (ResultSet rs = md.getImportedKeys(dvNameCanonical, null, viewName)) {
                        assertColumnValues(rs, expectedColumns,
                                Arrays.asList(expectedPKTableCat, expectedPKTableSchem, expectedPKTableName,
                                        expectedPKColumnName, expectedFKTableCat, expectedFKTableSchem,
                                        expectedFKTableName, expectedFKColumnName, expectedKeySeq));
                    }
                }

                // non-existent catalog
                try (ResultSet rs = md.getImportedKeys("UNKNOWN", null, null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent schema
                try (ResultSet rs = md.getImportedKeys(null, "UNKNOWN", null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent table name
                try (ResultSet rs = md.getImportedKeys(null, null, "UNKNOWN")) {
                    Assert.assertEquals(0, countRows(rs));
                }
            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    private void addExpectedColumnNamesForGetImportedKeys(String dvNameCanonical, String dsName,
            List<String> pkFkColumnNames, List<String> fkRefs, List<String> outPKTableCat, List<String> outPKTableSchem,
            List<String> outPKTableName, List<String> outPKColumnName, List<String> outFKTableCat,
            List<String> outFKTableSchem, List<String> outFKTableName, List<String> outFKColumnName,
            List<Integer> outKeySeq) {
        for (String fkRef : fkRefs) {
            for (int i = 0; i < pkFkColumnNames.size(); i++) {
                String pkFkColumn = pkFkColumnNames.get(i);
                outPKTableCat.add(dvNameCanonical);
                outPKTableSchem.add(null);
                outPKTableName.add(fkRef);
                outPKColumnName.add(pkFkColumn);
                outFKTableCat.add(dvNameCanonical);
                outFKTableSchem.add(null);
                outFKTableName.add(dsName);
                outFKColumnName.add(pkFkColumn);
                outKeySeq.add(i + 1);
            }
        }
    }

    public void testGetExportedKeys() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<List<String>> newDataverseList = new ArrayList<>();
            List<Pair<List<String>, String>> newDatasetList = new ArrayList<>();
            List<Pair<List<String>, String>> newViewList = new ArrayList<>();

            try {
                createDataversesDatasetsViews(s, newDataverseList, newDatasetList, newViewList);

                DatabaseMetaData md = c.getMetaData();

                List<String> expectedColumns = Arrays.asList(PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME,
                        FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ);
                List<String> expectedPKTableCat = new ArrayList<>();
                List<String> expectedPKTableSchem = new ArrayList<>();
                List<String> expectedPKTableName = new ArrayList<>();
                List<String> expectedPKColumnName = new ArrayList<>();
                List<String> expectedFKTableCat = new ArrayList<>();
                List<String> expectedFKTableSchem = new ArrayList<>();
                List<String> expectedFKTableName = new ArrayList<>();
                List<String> expectedFKColumnName = new ArrayList<>();
                List<Integer> expectedKeySeq = new ArrayList<>();

                // Test getExportedKeys() for a particular view
                for (int i = 0, n = newViewList.size(); i < n; i++) {
                    Pair<List<String>, String> p = newViewList.get(i);
                    List<String> dvName = p.first;
                    String dvNameCanonical = getCanonicalDataverseName(dvName);
                    String viewName = p.second;

                    expectedPKTableCat.clear();
                    expectedPKTableSchem.clear();
                    expectedPKTableName.clear();
                    expectedPKColumnName.clear();
                    expectedFKTableCat.clear();
                    expectedFKTableSchem.clear();
                    expectedFKTableName.clear();
                    expectedFKColumnName.clear();
                    expectedKeySeq.clear();

                    List<String> pkFkColumnNames = VIEW_COLUMN_NAMES.subList(0, DATASET_PK_LEN);
                    List<String> fkRefs = IntStream.range(i + 1, newViewList.size()).mapToObj(newViewList::get)
                            .filter(p2 -> p2.first.equals(dvName)).map(p2 -> p2.second).collect(Collectors.toList());

                    addExpectedColumnNamesForGetExportedKeys(dvNameCanonical, viewName, pkFkColumnNames, fkRefs,
                            expectedPKTableCat, expectedPKTableSchem, expectedPKTableName, expectedPKColumnName,
                            expectedFKTableCat, expectedFKTableSchem, expectedFKTableName, expectedFKColumnName,
                            expectedKeySeq);

                    try (ResultSet rs = md.getExportedKeys(dvNameCanonical, null, viewName)) {
                        assertColumnValues(rs, expectedColumns,
                                Arrays.asList(expectedPKTableCat, expectedPKTableSchem, expectedPKTableName,
                                        expectedPKColumnName, expectedFKTableCat, expectedFKTableSchem,
                                        expectedFKTableName, expectedFKColumnName, expectedKeySeq));
                    }
                }

                // non-existent catalog
                try (ResultSet rs = md.getExportedKeys("UNKNOWN", null, null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent schema
                try (ResultSet rs = md.getExportedKeys(null, "UNKNOWN", null)) {
                    Assert.assertEquals(0, countRows(rs));
                }
                // non-existent table name
                try (ResultSet rs = md.getExportedKeys(null, null, "UNKNOWN")) {
                    Assert.assertEquals(0, countRows(rs));
                }
            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    private void addExpectedColumnNamesForGetExportedKeys(String dvNameCanonical, String dsName,
            List<String> pkFkColumnNames, List<String> fkRefs, List<String> outPKTableCat, List<String> outPKTableSchem,
            List<String> outPKTableName, List<String> outPKColumnName, List<String> outFKTableCat,
            List<String> outFKTableSchem, List<String> outFKTableName, List<String> outFKColumnName,
            List<Integer> outKeySeq) {
        for (String fkRef : fkRefs) {
            for (int i = 0; i < pkFkColumnNames.size(); i++) {
                String pkFkColumn = pkFkColumnNames.get(i);
                outPKTableCat.add(dvNameCanonical);
                outPKTableSchem.add(null);
                outPKTableName.add(dsName);
                outPKColumnName.add(pkFkColumn);
                outFKTableCat.add(dvNameCanonical);
                outFKTableSchem.add(null);
                outFKTableName.add(fkRef);
                outFKColumnName.add(pkFkColumn);
                outKeySeq.add(i + 1);
            }
        }
    }

    public void testGetCrossReference() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            List<List<String>> newDataverseList = new ArrayList<>();
            List<Pair<List<String>, String>> newDatasetList = new ArrayList<>();
            List<Pair<List<String>, String>> newViewList = new ArrayList<>();

            try {
                createDataversesDatasetsViews(s, newDataverseList, newDatasetList, newViewList);

                DatabaseMetaData md = c.getMetaData();

                List<String> expectedColumns = Arrays.asList(PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME,
                        FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ);
                List<String> expectedPKTableCat = new ArrayList<>();
                List<String> expectedPKTableSchem = new ArrayList<>();
                List<String> expectedPKTableName = new ArrayList<>();
                List<String> expectedPKColumnName = new ArrayList<>();
                List<String> expectedFKTableCat = new ArrayList<>();
                List<String> expectedFKTableSchem = new ArrayList<>();
                List<String> expectedFKTableName = new ArrayList<>();
                List<String> expectedFKColumnName = new ArrayList<>();
                List<Integer> expectedKeySeq = new ArrayList<>();

                boolean testUnknown = true;
                // Test getCrossReference() for a particular view
                for (int i = 0, n = newViewList.size(); i < n; i++) {
                    Pair<List<String>, String> p = newViewList.get(i);
                    List<String> dvName = p.first;
                    String dvNameCanonical = getCanonicalDataverseName(dvName);
                    String viewName = p.second;

                    List<String> pkFkColumnNames = VIEW_COLUMN_NAMES.subList(0, DATASET_PK_LEN);
                    Iterator<String> fkRefIter = IntStream.range(i + 1, newViewList.size()).mapToObj(newViewList::get)
                            .filter(p2 -> p2.first.equals(dvName)).map(p2 -> p2.second).iterator();
                    boolean hasFkRefs = fkRefIter.hasNext();
                    while (fkRefIter.hasNext()) {
                        String fkRef = fkRefIter.next();

                        expectedPKTableCat.clear();
                        expectedPKTableSchem.clear();
                        expectedPKTableName.clear();
                        expectedPKColumnName.clear();
                        expectedFKTableCat.clear();
                        expectedFKTableSchem.clear();
                        expectedFKTableName.clear();
                        expectedFKColumnName.clear();
                        expectedKeySeq.clear();

                        addExpectedColumnNamesForGetCrossReference(dvNameCanonical, viewName, pkFkColumnNames, fkRef,
                                expectedPKTableCat, expectedPKTableSchem, expectedPKTableName, expectedPKColumnName,
                                expectedFKTableCat, expectedFKTableSchem, expectedFKTableName, expectedFKColumnName,
                                expectedKeySeq);

                        try (ResultSet rs =
                                md.getCrossReference(dvNameCanonical, null, viewName, dvNameCanonical, null, fkRef)) {
                            assertColumnValues(rs, expectedColumns,
                                    Arrays.asList(expectedPKTableCat, expectedPKTableSchem, expectedPKTableName,
                                            expectedPKColumnName, expectedFKTableCat, expectedFKTableSchem,
                                            expectedFKTableName, expectedFKColumnName, expectedKeySeq));
                        }
                    }

                    if (testUnknown && hasFkRefs) {
                        testUnknown = false;
                        // non-existent catalog
                        try (ResultSet rs =
                                md.getCrossReference(dvNameCanonical, null, viewName, "UNKNOWN", null, "UNKNOWN")) {
                            Assert.assertEquals(0, countRows(rs));
                        }
                        // non-existent schema
                        try (ResultSet rs = md.getCrossReference(dvNameCanonical, null, viewName, dvNameCanonical,
                                "UNKNOWN", "UNKNOWN")) {
                            Assert.assertEquals(0, countRows(rs));
                        }
                        // non-existent table name
                        try (ResultSet rs = md.getCrossReference(dvNameCanonical, null, viewName, dvNameCanonical, null,
                                "UNKNOWN")) {
                            Assert.assertEquals(0, countRows(rs));
                        }
                    }
                }

            } finally {
                dropDataverses(s, newDataverseList);
            }
        }
    }

    private void addExpectedColumnNamesForGetCrossReference(String dvNameCanonical, String dsName,
            List<String> pkFkColumnNames, String fkRef, List<String> outPKTableCat, List<String> outPKTableSchem,
            List<String> outPKTableName, List<String> outPKColumnName, List<String> outFKTableCat,
            List<String> outFKTableSchem, List<String> outFKTableName, List<String> outFKColumnName,
            List<Integer> outKeySeq) {
        for (int i = 0; i < pkFkColumnNames.size(); i++) {
            String pkFkColumn = pkFkColumnNames.get(i);
            outPKTableCat.add(dvNameCanonical);
            outPKTableSchem.add(null);
            outPKTableName.add(dsName);
            outPKColumnName.add(pkFkColumn);
            outFKTableCat.add(dvNameCanonical);
            outFKTableSchem.add(null);
            outFKTableName.add(fkRef);
            outFKColumnName.add(pkFkColumn);
            outKeySeq.add(i + 1);
        }
    }

    public void testGetTypeInfo() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getTypeInfo()) {
                int n = countRows(rs);
                Assert.assertTrue(String.valueOf(n), n > 10);
            }
        }
    }

    private static boolean isMetadataCatalog(ResultSet rs) throws SQLException {
        return METADATA_DATAVERSE_NAME.equals(rs.getString(TABLE_CAT));
    }

    private void createDataverses(Statement stmt, List<List<String>> outDataverseList) throws SQLException {
        for (String p1 : new String[] { "x", "y" }) {
            List<String> dv1 = Collections.singletonList(p1);
            stmt.execute(printCreateDataverse(dv1));
            outDataverseList.add(dv1);
            for (int p2i = 0; p2i <= 9; p2i++) {
                String p2 = "z" + p2i;
                List<String> dv2 = Arrays.asList(p1, p2);
                stmt.execute(printCreateDataverse(dv2));
                outDataverseList.add(dv2);
            }
        }
    }

    private void createDataversesDatasetsViews(Statement stmt, List<List<String>> outDataverseList,
            List<Pair<List<String>, String>> outDatasetList, List<Pair<List<String>, String>> outViewList)
            throws SQLException {
        for (String p1 : new String[] { "x", "y" }) {
            for (int p2i = 0; p2i < 2; p2i++) {
                String p2 = "z" + p2i;
                List<String> dv = Arrays.asList(p1, p2);
                stmt.execute(printCreateDataverse(dv));
                outDataverseList.add(dv);
                for (int i = 0; i < 3; i++) {
                    // create dataset
                    String datasetName = createDatasetName(i);
                    stmt.execute(printCreateDataset(dv, datasetName, DATASET_COLUMN_NAMES, DATASET_COLUMN_TYPES,
                            DATASET_PK_LEN));
                    outDatasetList.add(new Pair<>(dv, datasetName));
                    // create tabular view
                    String viewName = createViewName(i);
                    String viewQuery = "select r va, r vb, r vc from range(1,2) r";
                    List<String> fkRefs = IntStream.range(0, i).mapToObj(JdbcMetadataTester::createViewName)
                            .collect(Collectors.toList());
                    stmt.execute(printCreateView(dv, viewName, VIEW_COLUMN_NAMES, DATASET_COLUMN_TYPES, DATASET_PK_LEN,
                            fkRefs, viewQuery));
                    outViewList.add(new Pair<>(dv, viewName));
                }
            }
        }
    }

    private static String createDatasetName(int id) {
        return "t" + id;
    }

    private static String createViewName(int id) {
        return "v" + id;
    }

    private void dropDataverses(Statement stmt, List<List<String>> dataverseList) throws SQLException {
        for (List<String> dv : dataverseList) {
            stmt.execute(printDropDataverse(dv));
        }
    }

    private void assertColumnValues(ResultSet rs, String column, List<?> values) throws SQLException {
        assertColumnValues(rs, Collections.singletonList(column), Collections.singletonList(values));
    }

    private void assertColumnValues(ResultSet rs, List<String> columns, List<List<?>> values) throws SQLException {
        assertColumnValues(rs, columns, values, null);
    }

    private void assertColumnValues(ResultSet rs, List<String> columns, List<List<?>> values,
            JdbcPredicate<ResultSet> skipRowTest) throws SQLException {
        int columnCount = columns.size();
        Assert.assertEquals(columnCount, values.size());
        List<Iterator<?>> valueIters = values.stream().map(List::iterator).collect(Collectors.toList());
        while (rs.next()) {
            if (skipRowTest != null && skipRowTest.test(rs)) {
                continue;
            }
            for (int i = 0; i < columnCount; i++) {
                String column = columns.get(i);
                Object expectedValue = valueIters.get(i).next();
                Object actualValue;
                if (expectedValue instanceof String) {
                    actualValue = rs.getString(column);
                } else if (expectedValue instanceof Integer) {
                    actualValue = rs.getInt(column);
                } else if (expectedValue instanceof Long) {
                    actualValue = rs.getLong(column);
                } else {
                    actualValue = rs.getObject(column);
                }
                if (rs.wasNull()) {
                    Assert.assertNull(expectedValue);
                } else {
                    Assert.assertEquals(expectedValue, actualValue);
                }
            }
        }
        for (Iterator<?> i : valueIters) {
            if (i.hasNext()) {
                Assert.fail(String.valueOf(i.next()));
            }
        }
    }

    private int countRows(ResultSet rs) throws SQLException {
        int n = 0;
        while (rs.next()) {
            n++;
        }
        return n;
    }

    public void testWrapper() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData md = c.getMetaData();
            Assert.assertTrue(md.isWrapperFor(ADBDatabaseMetaData.class));
            Assert.assertNotNull(md.unwrap(ADBDatabaseMetaData.class));
        }
    }
}
