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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.asterix.jdbc.core.ADBResultSet;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.junit.Assert;

abstract class JdbcResultSetTester extends JdbcTester {

    protected abstract CloseablePair<Statement, ResultSet> executeQuery(Connection c, String query) throws SQLException;

    public void testLifecycle() throws SQLException {
        try (Connection c = createConnection()) {
            Pair<Statement, ResultSet> p = executeQuery(c, Q2);
            Statement s = p.getFirst();
            ResultSet rs = p.getSecond();
            Assert.assertFalse(rs.isClosed());
            Assert.assertSame(s, rs.getStatement());
            rs.close();
            Assert.assertTrue(rs.isClosed());

            // ok to call close() on a closed result set
            rs.close();
            Assert.assertTrue(rs.isClosed());
        }
    }

    // test that Statement.close() closes its ResultSet
    public void testAutoCloseOnStatementClose() throws SQLException {
        try (Connection c = createConnection()) {
            Pair<Statement, ResultSet> p = executeQuery(c, Q2);
            Statement s = p.getFirst();
            ResultSet rs = p.getSecond();
            Assert.assertFalse(rs.isClosed());
            s.close();
            Assert.assertTrue(rs.isClosed());
        }
    }

    // test that Connection.close() closes all Statements and their ResultSets
    public void testAutoCloseOnConnectionClose() throws SQLException {
        Connection c = createConnection();
        Pair<Statement, ResultSet> p1 = executeQuery(c, Q2);
        Statement s1 = p1.getFirst();
        ResultSet rs1 = p1.getSecond();
        Assert.assertFalse(rs1.isClosed());
        Pair<Statement, ResultSet> p2 = executeQuery(c, Q2);
        Statement s2 = p2.getFirst();
        ResultSet rs2 = p2.getSecond();
        Assert.assertFalse(rs2.isClosed());
        c.close();
        Assert.assertTrue(rs1.isClosed());
        Assert.assertTrue(s1.isClosed());
        Assert.assertTrue(rs2.isClosed());
        Assert.assertTrue(s2.isClosed());
    }

    public void testNavigation() throws SQLException {
        try (Connection c = createConnection()) {
            Pair<Statement, ResultSet> p = executeQuery(c, Q2);
            ResultSet rs = p.getSecond();
            Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            Assert.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            Assert.assertTrue(rs.isBeforeFirst());
            Assert.assertFalse(rs.isFirst());
            // Assert.assertFalse(rs.isLast()); -- Not supported
            Assert.assertFalse(rs.isAfterLast());
            Assert.assertEquals(0, rs.getRow());

            for (int r = 1; r <= 9; r++) {
                boolean next = rs.next();
                Assert.assertTrue(next);
                Assert.assertFalse(rs.isBeforeFirst());
                Assert.assertEquals(r == 1, rs.isFirst());
                Assert.assertFalse(rs.isAfterLast());
                Assert.assertEquals(r, rs.getRow());
            }

            boolean next = rs.next();
            Assert.assertFalse(next);
            Assert.assertFalse(rs.isBeforeFirst());
            Assert.assertFalse(rs.isFirst());
            Assert.assertTrue(rs.isAfterLast());
            Assert.assertEquals(0, rs.getRow());

            next = rs.next();
            Assert.assertFalse(next);

            rs.close();
            assertErrorOnClosed(rs, ResultSet::isBeforeFirst, "isBeforeFirst");
            assertErrorOnClosed(rs, ResultSet::isFirst, "isFirst");
            assertErrorOnClosed(rs, ResultSet::isAfterLast, "isAfterLast");
            assertErrorOnClosed(rs, ResultSet::getRow, "getRow");
            assertErrorOnClosed(rs, ResultSet::next, "next");
        }
    }

    public void testColumReadBasic() throws SQLException {
        String qProject = IntStream.range(1, 10).mapToObj(i -> String.format("r*10+%d as c%d", i, i))
                .collect(Collectors.joining(","));
        String q = String.format("select %s from range(1, 2) r order by r", qProject);
        try (Connection c = createConnection(); CloseablePair<Statement, ResultSet> p = executeQuery(c, q)) {
            ResultSet rs = p.getSecond();
            for (int r = 1; rs.next(); r++) {
                for (int col = 1; col < 10; col++) {
                    int expected = r * 10 + col;
                    Assert.assertEquals(expected, rs.getInt(col));
                    Assert.assertEquals(expected, rs.getInt("c" + col));
                    Assert.assertEquals(expected, rs.getInt(rs.findColumn("c" + col)));
                }
            }
        }
    }

    public void testColumnRead() throws SQLException, IOException {
        try (Connection c = createConnection(); CloseablePair<Statement, ResultSet> p = executeQuery(c, Q3)) {
            ResultSet rs = p.getSecond();
            for (int r = -1; rs.next(); r++) {
                int v = r * 2;
                verifyReadColumnOfNumericType(rs, 1, Q3_COLUMNS[0], v == 0 ? null : (byte) v);
                verifyReadColumnOfNumericType(rs, 2, Q3_COLUMNS[1], v == 0 ? null : (short) v);
                verifyReadColumnOfNumericType(rs, 3, Q3_COLUMNS[2], v == 0 ? null : v);
                verifyReadColumnOfNumericType(rs, 4, Q3_COLUMNS[3], v == 0 ? null : (long) v);
                verifyReadColumnOfNumericType(rs, 5, Q3_COLUMNS[4], v == 0 ? null : (float) v);
                verifyReadColumnOfNumericType(rs, 6, Q3_COLUMNS[5], v == 0 ? null : (double) v);
                verifyReadColumnOfStringType(rs, 7, Q3_COLUMNS[6], v == 0 ? null : "a" + v);
                verifyReadColumnOfBooleanType(rs, 8, Q3_COLUMNS[7], v == 0 ? null : v > 0);
                verifyReadColumnOfDateType(rs, 9, Q3_COLUMNS[8], v == 0 ? null : LocalDate.ofEpochDay(v));
                verifyReadColumnOfTimeType(rs, 10, Q3_COLUMNS[9], v == 0 ? null : LocalTime.ofSecondOfDay(v + 3));
                verifyReadColumnOfDatetimeType(rs, 11, Q3_COLUMNS[10],
                        v == 0 ? null : LocalDateTime.ofEpochSecond(v, 0, ZoneOffset.UTC));
                verifyReadColumnOfYearMonthDurationType(rs, 12, Q3_COLUMNS[11], v == 0 ? null : Period.ofMonths(v));
                verifyReadColumnOfDayTimeDurationType(rs, 13, Q3_COLUMNS[12], v == 0 ? null : Duration.ofMillis(v));
                verifyReadColumnOfDurationType(rs, 14, Q3_COLUMNS[13], v == 0 ? null : Period.ofMonths(v + 3),
                        v == 0 ? null : Duration.ofMillis(TimeUnit.SECONDS.toMillis(v + 3)));
                verifyReadColumnOfUuidType(rs, 15, Q3_COLUMNS[14],
                        v == 0 ? null : UUID.fromString("5c848e5c-6b6a-498f-8452-8847a295742" + (v + 3)));
            }
        }
    }

    public void testColumnMetadata() throws SQLException {
        try (Connection c = createConnection(); CloseablePair<Statement, ResultSet> p = executeQuery(c, Q3)) {
            ResultSet rs = p.getSecond();
            int expectedColumnCount = Q3_COLUMNS.length;
            ResultSetMetaData rsmd = rs.getMetaData();
            Assert.assertEquals(expectedColumnCount, rsmd.getColumnCount());
            for (int i = 1; i <= expectedColumnCount; i++) {
                String expectedColumnName = Q3_COLUMNS[i - 1];
                JDBCType expectedColumnTypeJdbc = Q3_COLUMN_TYPES_JDBC[i - 1];
                String expectedColumnTypeAdb = Q3_COLUMN_TYPES_ADB[i - 1];
                Class<?> expectedColumnTypeJava = Q3_COLUMN_TYPES_JAVA[i - 1];
                Assert.assertEquals(i, rs.findColumn(expectedColumnName));
                Assert.assertEquals(expectedColumnName, rsmd.getColumnName(i));
                Assert.assertEquals(expectedColumnTypeJdbc.getVendorTypeNumber().intValue(), rsmd.getColumnType(i));
                Assert.assertEquals(expectedColumnTypeAdb, rsmd.getColumnTypeName(i));
                Assert.assertEquals(expectedColumnTypeJava.getName(), rsmd.getColumnClassName(i));
            }
        }
    }

    private void verifyGetColumnAsByte(ResultSet rs, int columnIndex, String columnName, Number expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        byte expectedByte = expectedValue == null ? 0 : expectedValue.byteValue();
        byte v1 = rs.getByte(columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedByte, v1);
        byte v2 = rs.getByte(columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedByte, v2);
    }

    private void verifyGetColumnAsShort(ResultSet rs, int columnIndex, String columnName, Number expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        short expectedShort = expectedValue == null ? 0 : expectedValue.shortValue();
        short v1 = rs.getShort(columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedShort, v1);
        short v2 = rs.getShort(columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedShort, v2);
    }

    private void verifyGetColumnAsInt(ResultSet rs, int columnIndex, String columnName, Number expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        int expectedInt = expectedValue == null ? 0 : expectedValue.intValue();
        int v1 = rs.getInt(columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedInt, v1);
        int v2 = rs.getInt(columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedInt, v2);
    }

    private void verifyGetColumnAsLong(ResultSet rs, int columnIndex, String columnName, Number expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        long expectedLong = expectedValue == null ? 0 : expectedValue.longValue();
        long v1 = rs.getLong(columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedLong, v1);
        long v2 = rs.getLong(columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedLong, v2);
    }

    private void verifyGetColumnAsFloat(ResultSet rs, int columnIndex, String columnName, Number expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        float expectedFloat = expectedValue == null ? 0f : expectedValue.floatValue();
        float v1 = rs.getFloat(columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedFloat, v1, 0);
        float v2 = rs.getFloat(columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedFloat, v2, 0);
    }

    private void verifyGetColumnAsDouble(ResultSet rs, int columnIndex, String columnName, Number expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        double expectedDouble = expectedValue == null ? 0d : expectedValue.doubleValue();
        double v1 = rs.getDouble(columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedDouble, v1, 0);
        double v2 = rs.getDouble(columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedDouble, v2, 0);
    }

    private void verifyGetColumnAsDecimal(ResultSet rs, int columnIndex, String columnName, Number expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        BigDecimal expectedDecimal = expectedValue == null ? null : new BigDecimal(expectedValue.toString());
        int expectedDecimalScale = expectedValue == null ? 0 : expectedDecimal.scale();
        BigDecimal v1 = rs.getBigDecimal(columnIndex, expectedDecimalScale);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedDecimal, v1);
        BigDecimal v2 = rs.getBigDecimal(columnName, expectedDecimalScale);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedDecimal, v2);
    }

    private void verifyGetColumnAsBoolean(ResultSet rs, int columnIndex, String columnName, Boolean expectedValue)
            throws SQLException {
        boolean expectedNull = expectedValue == null;
        boolean expectedBoolean = expectedNull ? false : expectedValue;
        boolean v1 = rs.getBoolean(columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedBoolean, v1);
        boolean v2 = rs.getBoolean(columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        Assert.assertEquals(expectedBoolean, v2);
    }

    private void verifyGetColumnAsString(ResultSet rs, int columnIndex, String columnName, String expectedValue)
            throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, ResultSet::getString, ResultSet::getString);
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, ResultSet::getNString,
                ResultSet::getNString);
    }

    private void verifyGetColumnAsObject(ResultSet rs, int columnIndex, String columnName, Object expectedValue)
            throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, ResultSet::getObject, ResultSet::getObject);
    }

    private <V> void verifyGetColumnAsObject(ResultSet rs, int columnIndex, String columnName, V expectedValue,
            Function<Object, V> valueConverter) throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, valueConverter, ResultSet::getObject,
                ResultSet::getObject);
    }

    private <V> void verifyGetColumnAsObject(ResultSet rs, int columnIndex, String columnName, V expectedValue,
            Class<V> type) throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, ResultSet::getObject, ResultSet::getObject,
                type);
    }

    private <V, T> void verifyGetColumnAsObject(ResultSet rs, int columnIndex, String columnName, V expectedValue,
            Class<T> type, Function<T, V> valueConverter) throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, valueConverter, ResultSet::getObject,
                ResultSet::getObject, type);
    }

    private void verifyGetColumnAsSqlDate(ResultSet rs, int columnIndex, String columnName, LocalDate expectedValue)
            throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, java.sql.Date::toLocalDate,
                ResultSet::getDate, ResultSet::getDate);
    }

    private void verifyGetColumnAsSqlTime(ResultSet rs, int columnIndex, String columnName, LocalTime expectedValue)
            throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, java.sql.Time::toLocalTime,
                ResultSet::getTime, ResultSet::getTime);
    }

    private void verifyGetColumnAsSqlTimestamp(ResultSet rs, int columnIndex, String columnName,
            LocalDateTime expectedValue) throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, java.sql.Timestamp::toLocalDateTime,
                ResultSet::getTimestamp, ResultSet::getTimestamp);
    }

    private <V> void verifyGetColumnGeneric(ResultSet rs, int columnIndex, String columnName, V expectedValue,
            GetColumnByIndex<V> columnByIndexAccessor, GetColumnByName<V> columnByNameAccessor) throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, Function.identity(), columnByIndexAccessor,
                columnByNameAccessor);
    }

    private <V, T> void verifyGetColumnGeneric(ResultSet rs, int columnIndex, String columnName, V expectedValue,
            Function<T, V> valueConverter, GetColumnByIndex<T> columnByIndexAccessor,
            GetColumnByName<T> columnByNameAccessor) throws SQLException {
        boolean expectedNull = expectedValue == null;
        T v1 = columnByIndexAccessor.get(rs, columnIndex);
        Assert.assertEquals(expectedNull, rs.wasNull());
        if (expectedNull) {
            Assert.assertNull(v1);
        } else {
            Assert.assertEquals(expectedValue, valueConverter.apply(v1));
        }
        T v2 = columnByNameAccessor.get(rs, columnName);
        Assert.assertEquals(expectedNull, rs.wasNull());
        if (expectedNull) {
            Assert.assertNull(v2);
        } else {
            Assert.assertEquals(expectedValue, valueConverter.apply(v2));
        }
    }

    private <V, P> void verifyGetColumnGeneric(ResultSet rs, int columnIndex, String columnName, V expectedValue,
            GetColumnByIndexWithParam<V, P> columnByIndexAccessor, GetColumnByNameWithParam<V, P> columnByNameAccessor,
            P accessorParamValue) throws SQLException {
        verifyGetColumnGeneric(rs, columnIndex, columnName, expectedValue, Function.identity(), columnByIndexAccessor,
                columnByNameAccessor, accessorParamValue);
    }

    private <V, T, P> void verifyGetColumnGeneric(ResultSet rs, int columnIndex, String columnName, V expectedValue,
            Function<T, V> valueConverter, GetColumnByIndexWithParam<T, P> columnByIndexAccessor,
            GetColumnByNameWithParam<T, P> columnByNameAccessor, P accessorParamValue) throws SQLException {
        boolean expectedNull = expectedValue == null;
        T v1 = columnByIndexAccessor.get(rs, columnIndex, accessorParamValue);
        Assert.assertEquals(expectedNull, rs.wasNull());
        if (expectedNull) {
            Assert.assertNull(v1);
        } else {
            Assert.assertEquals(expectedValue, valueConverter.apply(v1));
        }
        T v2 = columnByNameAccessor.get(rs, columnName, accessorParamValue);
        Assert.assertEquals(expectedNull, rs.wasNull());
        if (expectedNull) {
            Assert.assertNull(v2);
        } else {
            Assert.assertEquals(expectedValue, valueConverter.apply(v2));
        }
    }

    private void verifyGetColumnAsCharacterStream(ResultSet rs, int columnIndex, String columnName,
            char[] expectedValue, GetColumnByIndex<Reader> columnByIndexAccessor,
            GetColumnByName<Reader> columnByNameAccessor) throws SQLException, IOException {
        boolean expectedNull = expectedValue == null;
        try (Reader s1 = columnByIndexAccessor.get(rs, columnIndex)) {
            Assert.assertEquals(expectedNull, rs.wasNull());
            if (expectedNull) {
                Assert.assertNull(s1);
            } else {
                Assert.assertArrayEquals(expectedValue, IOUtils.toCharArray(s1));
            }
        }
        try (Reader s2 = columnByNameAccessor.get(rs, columnName)) {
            Assert.assertEquals(expectedNull, rs.wasNull());
            if (expectedNull) {
                Assert.assertNull(s2);
            } else {
                Assert.assertArrayEquals(expectedValue, IOUtils.toCharArray(s2));
            }
        }
    }

    private void verifyGetColumnAsBinaryStream(ResultSet rs, int columnIndex, String columnName, byte[] expectedValue,
            GetColumnByIndex<InputStream> columnByIndexAccessor, GetColumnByName<InputStream> columnByNameAccessor)
            throws SQLException, IOException {
        boolean expectedNull = expectedValue == null;
        try (InputStream s1 = columnByIndexAccessor.get(rs, columnIndex)) {
            Assert.assertEquals(expectedNull, rs.wasNull());
            if (expectedNull) {
                Assert.assertNull(s1);
            } else {
                Assert.assertArrayEquals(expectedValue, IOUtils.toByteArray(s1));
            }
        }
        try (InputStream s2 = columnByNameAccessor.get(rs, columnName)) {
            Assert.assertEquals(expectedNull, rs.wasNull());
            if (expectedNull) {
                Assert.assertNull(s2);
            } else {
                Assert.assertArrayEquals(expectedValue, IOUtils.toByteArray(s2));
            }
        }
    }

    private void verifyReadColumnOfNumericType(ResultSet rs, int columnIndex, String columnName,
            Number expectedNumericValue) throws SQLException {
        String expectedStringValue = expectedNumericValue == null ? null : expectedNumericValue.toString();
        Byte expectedByteValue = expectedNumericValue == null ? null : expectedNumericValue.byteValue();
        Short expectedShortValue = expectedNumericValue == null ? null : expectedNumericValue.shortValue();
        Integer expectedIntValue = expectedNumericValue == null ? null : expectedNumericValue.intValue();
        Long expectedLongValue = expectedNumericValue == null ? null : expectedNumericValue.longValue();
        Float expectedFloatValue = expectedNumericValue == null ? null : expectedNumericValue.floatValue();
        Double expectedDoubleValue = expectedNumericValue == null ? null : expectedNumericValue.doubleValue();
        BigDecimal expectedDecimalValue =
                expectedNumericValue == null ? null : new BigDecimal(expectedStringValue.replace(".0", ""));
        Boolean expectedBooleanValue = toBoolean(expectedNumericValue);
        verifyGetColumnAsByte(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsShort(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsInt(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsLong(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsFloat(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsDouble(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsDecimal(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsBoolean(rs, columnIndex, columnName, expectedBooleanValue);
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedNumericValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedByteValue, Byte.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedShortValue, Short.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedIntValue, Integer.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedLongValue, Long.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedFloatValue, Float.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDoubleValue, Double.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDecimalValue, BigDecimal.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedBooleanValue, Boolean.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue, String.class);
    }

    private void verifyReadColumnOfStringType(ResultSet rs, int columnIndex, String columnName,
            String expectedStringValue) throws SQLException, IOException {
        char[] expectedCharArray = expectedStringValue == null ? null : expectedStringValue.toCharArray();
        byte[] expectedUtf8Array =
                expectedStringValue == null ? null : expectedStringValue.getBytes(StandardCharsets.UTF_8);
        byte[] expectedUtf16Array =
                expectedStringValue == null ? null : expectedStringValue.getBytes(StandardCharsets.UTF_16);
        byte[] expectedAsciiArray =
                expectedStringValue == null ? null : expectedStringValue.getBytes(StandardCharsets.US_ASCII);
        verifyGetColumnAsCharacterStream(rs, columnIndex, columnName, expectedCharArray, ResultSet::getCharacterStream,
                ResultSet::getCharacterStream);
        verifyGetColumnAsCharacterStream(rs, columnIndex, columnName, expectedCharArray, ResultSet::getNCharacterStream,
                ResultSet::getNCharacterStream);
        verifyGetColumnAsBinaryStream(rs, columnIndex, columnName, expectedUtf8Array, ResultSet::getBinaryStream,
                ResultSet::getBinaryStream);
        verifyGetColumnAsBinaryStream(rs, columnIndex, columnName, expectedUtf16Array, ResultSet::getUnicodeStream,
                ResultSet::getUnicodeStream);
        verifyGetColumnAsBinaryStream(rs, columnIndex, columnName, expectedAsciiArray, ResultSet::getAsciiStream,
                ResultSet::getAsciiStream);
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue, String.class);
    }

    private void verifyReadColumnOfBooleanType(ResultSet rs, int columnIndex, String columnName,
            Boolean expectedBooleanValue) throws SQLException {
        Number expectedNumberValue = expectedBooleanValue == null ? null : expectedBooleanValue ? 1 : 0;
        String expectedStringValue = expectedBooleanValue == null ? null : Boolean.toString(expectedBooleanValue);
        verifyGetColumnAsBoolean(rs, columnIndex, columnName, expectedBooleanValue);
        verifyGetColumnAsByte(rs, columnIndex, columnName, expectedNumberValue);
        verifyGetColumnAsShort(rs, columnIndex, columnName, expectedNumberValue);
        verifyGetColumnAsInt(rs, columnIndex, columnName, expectedNumberValue);
        verifyGetColumnAsLong(rs, columnIndex, columnName, expectedNumberValue);
        verifyGetColumnAsFloat(rs, columnIndex, columnName, expectedNumberValue);
        verifyGetColumnAsDouble(rs, columnIndex, columnName, expectedNumberValue);
        verifyGetColumnAsDecimal(rs, columnIndex, columnName, expectedNumberValue);
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedBooleanValue);
    }

    private void verifyReadColumnOfDateType(ResultSet rs, int columnIndex, String columnName,
            LocalDate expectedDateValue) throws SQLException {
        LocalDateTime expectedDateTimeValue = expectedDateValue == null ? null : expectedDateValue.atStartOfDay();
        String expectedStringValue = expectedDateValue == null ? null : expectedDateValue.toString();
        verifyGetColumnAsSqlDate(rs, columnIndex, columnName, expectedDateValue);
        verifyGetColumnAsSqlTimestamp(rs, columnIndex, columnName, expectedDateTimeValue);
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateValue, v -> ((java.sql.Date) v).toLocalDate());
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateValue, java.sql.Date.class, Date::toLocalDate);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateTimeValue, java.sql.Timestamp.class,
                Timestamp::toLocalDateTime);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateValue, LocalDate.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateTimeValue, LocalDateTime.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue, String.class);
    }

    private void verifyReadColumnOfTimeType(ResultSet rs, int columnIndex, String columnName,
            LocalTime expectedTimeValue) throws SQLException {
        String expectedStringValue = expectedTimeValue == null ? null : expectedTimeValue.toString();
        verifyGetColumnAsSqlTime(rs, columnIndex, columnName, expectedTimeValue);
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedTimeValue, v -> ((java.sql.Time) v).toLocalTime());
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedTimeValue, java.sql.Time.class,
                java.sql.Time::toLocalTime);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedTimeValue, LocalTime.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue, String.class);
    }

    private void verifyReadColumnOfDatetimeType(ResultSet rs, int columnIndex, String columnName,
            LocalDateTime expectedDateTimeValue) throws SQLException {
        LocalDate expectedDateValue = expectedDateTimeValue == null ? null : expectedDateTimeValue.toLocalDate();
        LocalTime expectedTimeValue = expectedDateTimeValue == null ? null : expectedDateTimeValue.toLocalTime();
        String expectedStringValue = expectedDateTimeValue == null ? null : expectedDateTimeValue.toString();
        verifyGetColumnAsSqlTimestamp(rs, columnIndex, columnName, expectedDateTimeValue);
        verifyGetColumnAsSqlDate(rs, columnIndex, columnName, expectedDateValue);
        verifyGetColumnAsSqlTime(rs, columnIndex, columnName, expectedTimeValue);
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateTimeValue,
                v -> ((java.sql.Timestamp) v).toLocalDateTime());
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateTimeValue, java.sql.Timestamp.class,
                java.sql.Timestamp::toLocalDateTime);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateValue, java.sql.Date.class,
                java.sql.Date::toLocalDate);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedTimeValue, java.sql.Time.class,
                java.sql.Time::toLocalTime);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateTimeValue, LocalDateTime.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDateValue, LocalDate.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedTimeValue, LocalTime.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue, String.class);
    }

    private void verifyReadColumnOfYearMonthDurationType(ResultSet rs, int columnIndex, String columnName,
            Period expectedPeriodValue) throws SQLException {
        String expectedStringValue = expectedPeriodValue == null ? null : expectedPeriodValue.toString();
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedPeriodValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedPeriodValue, Period.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue, String.class);
    }

    private void verifyReadColumnOfDayTimeDurationType(ResultSet rs, int columnIndex, String columnName,
            Duration expectedDurationValue) throws SQLException {
        String expectedStringValue = expectedDurationValue == null ? null : expectedDurationValue.toString();
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDurationValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDurationValue, Duration.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue, String.class);
    }

    private void verifyReadColumnOfDurationType(ResultSet rs, int columnIndex, String columnName,
            Period expectedPeriodValue, Duration expectedDurationValue) throws SQLException {
        String expectedStringValue = expectedPeriodValue == null && expectedDurationValue == null ? null
                : expectedPeriodValue + String.valueOf(expectedDurationValue).substring(1);
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedPeriodValue, Period.class);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedDurationValue, Duration.class);
    }

    private void verifyReadColumnOfUuidType(ResultSet rs, int columnIndex, String columnName, UUID expectedUuidValue)
            throws SQLException {
        String expectedStringValue = expectedUuidValue == null ? null : expectedUuidValue.toString();
        verifyGetColumnAsString(rs, columnIndex, columnName, expectedStringValue);
        verifyGetColumnAsObject(rs, columnIndex, columnName, expectedUuidValue);
    }

    public void testWrapper() throws SQLException {
        try (Connection c = createConnection(); CloseablePair<Statement, ResultSet> p = executeQuery(c, Q2)) {
            ResultSet rs = p.getSecond();
            Assert.assertTrue(rs.isWrapperFor(ADBResultSet.class));
            Assert.assertNotNull(rs.unwrap(ADBResultSet.class));
        }
    }

    interface GetColumnByIndex<R> {
        R get(ResultSet rs, int columnIndex) throws SQLException;
    }

    interface GetColumnByIndexWithParam<R, T> {
        R get(ResultSet rs, int columnIndex, T param) throws SQLException;
    }

    interface GetColumnByName<R> {
        R get(ResultSet rs, String columnName) throws SQLException;
    }

    interface GetColumnByNameWithParam<R, T> {
        R get(ResultSet rs, String columnName, T param) throws SQLException;
    }

    static Boolean toBoolean(Number v) {
        if (v == null) {
            return null;
        }
        switch (v.toString()) {
            case "0":
            case "0.0":
                return false;
            default:
                return true;
        }
    }

    static class JdbcPreparedStatementResultSetTester extends JdbcResultSetTester {
        @Override
        protected CloseablePair<Statement, ResultSet> executeQuery(Connection c, String query) throws SQLException {
            PreparedStatement s = c.prepareStatement(query);
            try {
                ResultSet rs = s.executeQuery();
                return new CloseablePair<>(s, rs);
            } catch (SQLException e) {
                s.close();
                throw e;
            }
        }
    }

    static class JdbcStatementResultSetTester extends JdbcResultSetTester {
        @Override
        protected CloseablePair<Statement, ResultSet> executeQuery(Connection c, String query) throws SQLException {
            Statement s = c.createStatement();
            try {
                ResultSet rs = s.executeQuery(query);
                return new CloseablePair<>(s, rs);
            } catch (SQLException e) {
                s.close();
                throw e;
            }
        }
    }
}
