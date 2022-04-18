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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiPredicate;

import org.junit.Assert;

class JdbcStatementParameterTester extends JdbcTester {

    public void testParameterBinding() throws SQLException {
        String[] sqlppValues = new String[] { "int8('10')", "int16('20')", "int32('30')", "int64('40')", "float('1.5')",
                "double('2.25')", "true", "'abc'", "date('2000-10-20')", "time('02:03:04')",
                "datetime('2000-10-20T02:03:04')", "get_year_month_duration(duration_from_months(2))",
                "get_day_time_duration(duration_from_ms(1234))", "uuid('5c848e5c-6b6a-498f-8452-8847a2957421')" };
        try (Connection c = createConnection()) {
            Byte i1 = (byte) 10;
            verifyParameterBinding(c, sqlppValues, i1, PreparedStatement::setByte, ResultSet::getByte);
            verifyParameterBinding(c, sqlppValues, i1, PreparedStatement::setObject, ResultSet::getByte);

            Short i2 = (short) 20;
            verifyParameterBinding(c, sqlppValues, i2, PreparedStatement::setShort, ResultSet::getShort);
            verifyParameterBinding(c, sqlppValues, i2, PreparedStatement::setObject, ResultSet::getShort);

            Integer i4 = 30;
            verifyParameterBinding(c, sqlppValues, i4, PreparedStatement::setInt, ResultSet::getInt);
            verifyParameterBinding(c, sqlppValues, i4, PreparedStatement::setObject, ResultSet::getInt);

            Long i8 = 40L;
            verifyParameterBinding(c, sqlppValues, i8, PreparedStatement::setLong, ResultSet::getLong);
            verifyParameterBinding(c, sqlppValues, i8, PreparedStatement::setObject, ResultSet::getLong);

            Float r4 = 1.5f;
            verifyParameterBinding(c, sqlppValues, r4, PreparedStatement::setFloat, ResultSet::getFloat);
            verifyParameterBinding(c, sqlppValues, r4, PreparedStatement::setObject, ResultSet::getFloat);

            Double r8 = 2.25;
            verifyParameterBinding(c, sqlppValues, r8, PreparedStatement::setDouble, ResultSet::getDouble);
            verifyParameterBinding(c, sqlppValues, r8, PreparedStatement::setObject, ResultSet::getDouble);

            BigDecimal dec = new BigDecimal("2.25");
            verifyParameterBinding(c, sqlppValues, dec, PreparedStatement::setBigDecimal, ResultSet::getBigDecimal);
            verifyParameterBinding(c, sqlppValues, dec, PreparedStatement::setObject, ResultSet::getBigDecimal);

            Boolean b = true;
            verifyParameterBinding(c, sqlppValues, b, PreparedStatement::setBoolean, ResultSet::getBoolean);
            verifyParameterBinding(c, sqlppValues, b, PreparedStatement::setObject, ResultSet::getBoolean);

            String s = "abc";
            verifyParameterBinding(c, sqlppValues, s, PreparedStatement::setString, ResultSet::getString);
            verifyParameterBinding(c, sqlppValues, s, PreparedStatement::setObject, ResultSet::getString);
            verifyParameterBinding(c, sqlppValues, s, PreparedStatement::setNString, ResultSet::getString);

            LocalDate date = LocalDate.of(2000, 10, 20);
            verifyParameterBinding(c, sqlppValues, java.sql.Date.valueOf(date), PreparedStatement::setDate,
                    ResultSet::getDate);
            verifyParameterBinding(c, sqlppValues, java.sql.Date.valueOf(date), PreparedStatement::setObject,
                    ResultSet::getDate);
            verifyParameterBinding(c, sqlppValues, date, PreparedStatement::setObject,
                    (rs, i) -> rs.getObject(i, LocalDate.class));

            LocalTime time = LocalTime.of(2, 3, 4);
            verifyParameterBinding(c, sqlppValues, java.sql.Time.valueOf(time), PreparedStatement::setTime,
                    ResultSet::getTime, JdbcStatementParameterTester::sqlTimeEquals);
            verifyParameterBinding(c, sqlppValues, java.sql.Time.valueOf(time), PreparedStatement::setObject,
                    ResultSet::getTime, JdbcStatementParameterTester::sqlTimeEquals);
            verifyParameterBinding(c, sqlppValues, time, PreparedStatement::setObject,
                    (rs, i) -> rs.getObject(i, LocalTime.class));

            LocalDateTime datetime = LocalDateTime.of(date, time);
            verifyParameterBinding(c, sqlppValues, java.sql.Timestamp.valueOf(datetime),
                    PreparedStatement::setTimestamp, ResultSet::getTimestamp);
            verifyParameterBinding(c, sqlppValues, java.sql.Timestamp.valueOf(datetime), PreparedStatement::setObject,
                    ResultSet::getTimestamp);
            verifyParameterBinding(c, sqlppValues, datetime, PreparedStatement::setObject,
                    (rs, i) -> rs.getObject(i, LocalDateTime.class));

            Period ymDuration = Period.ofMonths(2);
            verifyParameterBinding(c, sqlppValues, ymDuration, PreparedStatement::setObject,
                    (rs, i) -> rs.getObject(i, Period.class));

            Duration dtDuration = Duration.ofMillis(1234);
            verifyParameterBinding(c, sqlppValues, dtDuration, PreparedStatement::setObject,
                    (rs, i) -> rs.getObject(i, Duration.class));

            UUID uuid = UUID.fromString("5c848e5c-6b6a-498f-8452-8847a2957421");
            verifyParameterBinding(c, sqlppValues, uuid, PreparedStatement::setObject, ResultSet::getObject);
        }
    }

    private <T> void verifyParameterBinding(Connection c, String[] sqlppValues, T value, SetParameterByIndex<T> setter,
            JdbcResultSetTester.GetColumnByIndex<T> getter) throws SQLException {
        verifyParameterBinding(c, sqlppValues, value, setter, getter, Objects::equals);
    }

    private <T> void verifyParameterBinding(Connection c, String[] sqlppValues, T value, SetParameterByIndex<T> setter,
            JdbcResultSetTester.GetColumnByIndex<T> getter, BiPredicate<T, T> cmp) throws SQLException {
        try (PreparedStatement s =
                c.prepareStatement(String.format("select ? from [%s] v where v = ?", String.join(",", sqlppValues)))) {
            for (int i = 1; i <= 2; i++) {
                setter.set(s, i, value);
            }
            try (ResultSet rs = s.executeQuery()) {
                if (rs.next()) {
                    T outValue = getter.get(rs, 1);
                    if (!cmp.test(value, outValue)) {
                        Assert.fail(String.format("%s != %s", value, outValue));
                    }
                } else {
                    Assert.fail(String.format("Empty result (expected value '%s' was not returned)", value));
                }
            }
        }
    }

    public void testParameterMetadata() throws SQLException {
        String q = "select r from range(1, 10) r where r = ? or r = ? or r = ?";
        int paramCount = 3;
        try (Connection c = createConnection(); PreparedStatement s = c.prepareStatement(q)) {
            ParameterMetaData pmd = s.getParameterMetaData();
            Assert.assertEquals(paramCount, pmd.getParameterCount());
            for (int i = 1; i <= paramCount; i++) {
                Assert.assertEquals(JDBCType.OTHER.getVendorTypeNumber().intValue(), pmd.getParameterType(i));
                Assert.assertEquals("any", pmd.getParameterTypeName(i));
                Assert.assertEquals(ParameterMetaData.parameterModeIn, pmd.getParameterMode(i));
            }
        }
    }

    interface SetParameterByIndex<T> {
        void set(PreparedStatement s, int paramIndex, T paramValue) throws SQLException;
    }

    private static boolean sqlTimeEquals(java.sql.Time v1, java.sql.Time v2) {
        // java.sql.Time.equals() compares millis since epoch,
        // but we only want to compare time components
        return v1.toLocalTime().equals(v2.toLocalTime());
    }
}
