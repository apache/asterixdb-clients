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
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hyracks.algebricks.common.utils.Pair;
import org.junit.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

abstract class JdbcTester {

    static final String DEFAULT_DATAVERSE_NAME = "Default";

    static final String METADATA_DATAVERSE_NAME = "Metadata";

    static final List<String> BUILT_IN_DATAVERSE_NAMES = Arrays.asList(DEFAULT_DATAVERSE_NAME, METADATA_DATAVERSE_NAME);

    static final String SQL_STATE_CONNECTION_CLOSED = "08003";

    static final char IDENTIFIER_QUOTE = '`';

    static final int V1 = 42;

    static final String Q1 = printSelect(V1);

    static final String Q2 = "select r x, r * 11 y from range(1, 9) r order by r";

    static final String Q3_PROJECT = "int8(v) c1_i1, int16(v) c2_i2, int32(v) c3_i4, int64(v) c4_i8, float(v) c5_r4, "
            + "double(v) c6_r8, 'a' || string(v) c7_s, boolean(v+2) c8_b, date_from_unix_time_in_days(v) c9_d, "
            + "time_from_unix_time_in_ms((v+3)*1000) c10_t, datetime_from_unix_time_in_secs(v) c11_dt,"
            + "get_year_month_duration(duration_from_months(v)) c12_um, "
            + "get_day_time_duration(duration_from_ms(v)) c13_ut, "
            + "duration('P'||string(v+3)||'MT'||string(v+3)||'S') c14_uu, "
            + "uuid('5c848e5c-6b6a-498f-8452-8847a295742' || string(v+3)) c15_id";

    static final String Q3 = String.format("select %s from range(-1, 1) r let v=nullif(r,0)*2 order by r", Q3_PROJECT);

    static String[] Q3_COLUMNS = new String[] { "c1_i1", "c2_i2", "c3_i4", "c4_i8", "c5_r4", "c6_r8", "c7_s", "c8_b",
            "c9_d", "c10_t", "c11_dt", "c12_um", "c13_ut", "c14_uu", "c15_id" };

    static JDBCType[] Q3_COLUMN_TYPES_JDBC = new JDBCType[] { JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER,
            JDBCType.BIGINT, JDBCType.REAL, JDBCType.DOUBLE, JDBCType.VARCHAR, JDBCType.BOOLEAN, JDBCType.DATE,
            JDBCType.TIME, JDBCType.TIMESTAMP, JDBCType.OTHER, JDBCType.OTHER, JDBCType.OTHER, JDBCType.OTHER };

    static String[] Q3_COLUMN_TYPES_ADB = new String[] { "int8", "int16", "int32", "int64", "float", "double", "string",
            "boolean", "date", "time", "datetime", "year-month-duration", "day-time-duration", "duration", "uuid" };

    static Class<?>[] Q3_COLUMN_TYPES_JAVA = new Class<?>[] { Byte.class, Short.class, Integer.class, Long.class,
            Float.class, Double.class, String.class, Boolean.class, java.sql.Date.class, java.sql.Time.class,
            java.sql.Timestamp.class, Period.class, Duration.class, String.class, UUID.class };

    protected JdbcTestContext testContext;

    protected JdbcTester() {
    }

    void setTestContext(JdbcTestContext testContext) {
        this.testContext = Objects.requireNonNull(testContext);
    }

    static JdbcTestContext createTestContext(String host, int port) {
        return new JdbcTestContext(host, port);
    }

    protected Connection createConnection() throws SQLException {
        return DriverManager.getConnection(testContext.getJdbcUrl());
    }

    protected Connection createConnection(String dataverseName) throws SQLException {
        return createConnection(Collections.singletonList(dataverseName));
    }

    protected Connection createConnection(List<String> dataverseName) throws SQLException {
        return DriverManager.getConnection(testContext.getJdbcUrl(getCanonicalDataverseName(dataverseName)));
    }

    protected static String getCanonicalDataverseName(List<String> dataverseName) {
        return String.join("/", dataverseName);
    }

    protected static String printDataverseName(List<String> dataverseName) {
        return dataverseName.stream().map(JdbcTester::printIdentifier).collect(Collectors.joining("."));
    }

    protected static String printIdentifier(String ident) {
        return IDENTIFIER_QUOTE + ident + IDENTIFIER_QUOTE;
    }

    protected static String printCreateDataverse(List<String> dataverseName) {
        return String.format("create dataverse %s", printDataverseName(dataverseName));
    }

    protected static String printDropDataverse(List<String> dataverseName) {
        return String.format("drop dataverse %s", printDataverseName(dataverseName));
    }

    protected static String printCreateDataset(List<String> dataverseName, String datasetName) {
        return String.format("create dataset %s.%s(_id uuid) open type primary key _id autogenerated",
                printDataverseName(dataverseName), printIdentifier(datasetName));
    }

    protected static String printCreateDataset(List<String> dataverseName, String datasetName, List<String> fieldNames,
            List<String> fieldTypes, int pkLen) {
        return String.format("create dataset %s.%s(%s) open type primary key %s", printDataverseName(dataverseName),
                printIdentifier(datasetName), printSchema(fieldNames, fieldTypes),
                printIdentifierList(fieldNames.subList(0, pkLen)));
    }

    protected static String printCreateView(List<String> dataverseName, String viewName, List<String> fieldNames,
            List<String> fieldTypes, int pkLen, List<String> fkRefs, String viewQuery) {
        List<String> pkFieldNames = fieldNames.subList(0, pkLen);
        String pkDecl = String.format(" primary key (%s) not enforced", printIdentifierList(pkFieldNames));
        String fkDecl =
                fkRefs.stream()
                        .map(fkRef -> String.format("foreign key (%s) references %s not enforced",
                                printIdentifierList(pkFieldNames), printIdentifier(fkRef)))
                        .collect(Collectors.joining(" "));
        return String.format("create view %s.%s(%s) default null %s %s as %s", printDataverseName(dataverseName),
                printIdentifier(viewName), printSchema(fieldNames, fieldTypes), pkDecl, fkDecl, viewQuery);
    }

    protected static String printSchema(List<String> fieldNames, List<String> fieldTypes) {
        StringBuilder schema = new StringBuilder(128);
        for (int i = 0, n = fieldNames.size(); i < n; i++) {
            if (i > 0) {
                schema.append(',');
            }
            schema.append(printIdentifier(fieldNames.get(i))).append(' ').append(fieldTypes.get(i));
        }
        return schema.toString();
    }

    protected static String printIdentifierList(List<String> fieldNames) {
        return fieldNames.stream().map(JdbcTester::printIdentifier).collect(Collectors.joining(","));
    }

    protected static String printInsert(List<String> dataverseName, String datasetName, ArrayNode values) {
        return String.format("insert into %s.%s (%s)", printDataverseName(dataverseName), printIdentifier(datasetName),
                values);
    }

    protected static String printSelect(Object... values) {
        return String.format("select %s", Arrays.stream(values).map(String::valueOf).collect(Collectors.joining(",")));
    }

    protected static ArrayNode dataGen(String fieldName1, Object... data1) {
        ObjectMapper om = new ObjectMapper();
        ArrayNode values = om.createArrayNode();
        for (Object v : data1) {
            ObjectNode obj = om.createObjectNode();
            obj.putPOJO(fieldName1, v);
            values.add(obj);
        }
        return values;
    }

    protected static <T> void assertErrorOnClosed(T param, JdbcConnectionTester.JdbcRunnable<T> cmd,
            String description) {
        try {
            cmd.run(param);
            Assert.fail(String.format("Unexpected: %s succeeded on a closed %s", description,
                    param.getClass().getSimpleName()));
        } catch (SQLException e) {
            String msg = e.getMessage();
            Assert.assertTrue(msg, msg.contains("closed"));
        }
    }

    static class JdbcTestContext {

        private static final String JDBC_URL_TEMPLATE = "jdbc:asterixdb://%s:%d";

        private final String jdbcUrl;

        private JdbcTestContext(String host, int port) {
            jdbcUrl = String.format(JDBC_URL_TEMPLATE, host, port);
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public String getJdbcUrl(String dataverseName) {
            return jdbcUrl + '/' + dataverseName;
        }
    }

    interface JdbcRunnable<T> {
        void run(T param) throws SQLException;
    }

    interface JdbcPredicate<T> {
        boolean test(T param) throws SQLException;
    }

    static class CloseablePair<K extends AutoCloseable, V extends AutoCloseable> extends Pair<K, V>
            implements AutoCloseable {
        CloseablePair(K first, V second) {
            super(first, second);
        }

        @Override
        public void close() throws SQLException {
            try {
                if (second != null) {
                    try {
                        second.close();
                    } catch (SQLException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new SQLException(e);
                    }
                }
            } finally {
                if (first != null) {
                    try {
                        first.close();
                    } catch (SQLException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new SQLException(e);
                    }
                }
            }
        }
    }
}
