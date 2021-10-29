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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.ArrayNode;

public abstract class ADBProtocolBase {

    public static final String STATEMENT = "statement";
    public static final String ARGS = "args";
    public static final String MODE = "mode";
    public static final String READ_ONLY = "readonly";
    public static final String DATAVERSE = "dataverse";
    public static final String TIMEOUT = "timeout";
    public static final String SIGNATURE = "signature";
    public static final String COMPILE_ONLY = "compile-only";
    public static final String CLIENT_TYPE = "client-type";
    public static final String PLAN_FORMAT = "plan-format";
    public static final String MAX_WARNINGS = "max-warnings";
    public static final String SQL_COMPAT = "sql-compat";
    public static final String CLIENT_CONTEXT_ID = "client_context_id";

    public static final String MODE_DEFERRED = "deferred";
    public static final String CLIENT_TYPE_JDBC = "jdbc";
    public static final String RESULTS = "results";
    public static final String FORMAT_LOSSLESS_ADM = "lossless-adm";
    public static final String PLAN_FORMAT_STRING = "string";

    private static final String DEFAULT_DATAVERSE = "Default";
    private static final String OPTIONAL_TYPE_SUFFIX = "?";
    private static final String EXPLAIN_ONLY_RESULT_COLUMN_NAME = "$1";

    private static final Pattern DATABASE_VERSION_PATTERN =
            Pattern.compile("(?<name>[^/]+)(?:/(?<ver>(?:(?<vermj>\\d+)(?:\\.(?<vermn>\\d+))?)?.*))?");

    protected final ADBDriverContext driverContext;
    protected final String user;
    protected final int maxWarnings;

    protected ADBProtocolBase(ADBDriverContext driverContext, Map<ADBDriverProperty, Object> params) {
        this.driverContext = Objects.requireNonNull(driverContext);
        this.user = (String) ADBDriverProperty.Common.USER.fetchPropertyValue(params);
        Number maxWarningsNum = (Number) ADBDriverProperty.Common.MAX_WARNINGS.fetchPropertyValue(params);
        this.maxWarnings = Math.max(maxWarningsNum.intValue(), 0);
    }

    public abstract String connect() throws SQLException;

    public abstract void close() throws SQLException;

    public abstract boolean ping(int timeoutSeconds);

    public abstract QueryServiceResponse submitStatement(String sql, List<?> args, UUID executionId,
            SubmitStatementOptions options) throws SQLException;

    public abstract JsonParser fetchResult(QueryServiceResponse response) throws SQLException;

    public abstract void cancelRunningStatement(UUID executionId) throws SQLException;

    public String getUser() {
        return user;
    }

    public ADBDriverContext getDriverContext() {
        return driverContext;
    }

    public ADBErrorReporter getErrorReporter() {
        return getDriverContext().getErrorReporter();
    }

    public Logger getLogger() {
        return getDriverContext().getLogger();
    }

    public SubmitStatementOptions createSubmitStatementOptions() {
        return new SubmitStatementOptions();
    }

    public int getUpdateCount(QueryServiceResponse response) {
        // TODO:need to get update count through the response
        return isStatementCategory(response, QueryServiceResponse.StatementCategory.UPDATE) ? 1 : 0;
    }

    public ArrayNode fetchExplainOnlyResult(QueryServiceResponse response, Function<String, String> lineConverter)
            throws SQLException {
        if (response.results == null || response.results.isEmpty()) {
            throw getErrorReporter().errorInProtocol();
        }
        Object v = response.results.get(0);
        if (!(v instanceof String)) {
            throw getErrorReporter().errorInProtocol();
        }
        try (BufferedReader br = new BufferedReader(new StringReader(v.toString()))) {
            ArrayNode arrayNode = (ArrayNode) getDriverContext().getGenericObjectReader().createArrayNode();
            String line;
            while ((line = br.readLine()) != null) {
                arrayNode.addObject().put(EXPLAIN_ONLY_RESULT_COLUMN_NAME, lineConverter.apply(line));
            }
            return arrayNode;
        } catch (IOException e) {
            throw getErrorReporter().errorInResultHandling(e);
        }
    }

    public boolean isStatementCategory(QueryServiceResponse response, QueryServiceResponse.StatementCategory category) {
        return response.plans != null && category.equals(response.plans.statementCategory);
    }

    public SQLException getErrorIfExists(QueryServiceResponse response) {
        if (response.errors != null && !response.errors.isEmpty()) {
            QueryServiceResponse.Message err = response.errors.get(0);
            return new SQLException(err.msg, null, err.code);
        }
        return null;
    }

    public List<QueryServiceResponse.Message> getWarningIfExists(QueryServiceResponse response) {
        return response.warnings != null && !response.warnings.isEmpty() ? response.warnings : null;
    }

    public SQLWarning createSQLWarning(List<QueryServiceResponse.Message> warnings) {
        SQLWarning sqlWarning = null;
        ListIterator<QueryServiceResponse.Message> i = warnings.listIterator(warnings.size());
        while (i.hasPrevious()) {
            QueryServiceResponse.Message w = i.previous();
            SQLWarning sw = new SQLWarning(w.msg, null, w.code);
            if (sqlWarning != null) {
                sw.setNextWarning(sqlWarning);
            }
            sqlWarning = sw;
        }
        return sqlWarning;
    }

    public List<ADBColumn> getColumns(QueryServiceResponse response) throws SQLException {
        if (isExplainOnly(response)) {
            return Collections.singletonList(new ADBColumn(EXPLAIN_ONLY_RESULT_COLUMN_NAME, ADBDatatype.STRING, false));
        }
        QueryServiceResponse.Signature signature = response.signature;
        if (signature == null) {
            throw getErrorReporter().errorInProtocol();
        }
        List<String> nameList = signature.name;
        List<String> typeList = signature.type;
        if (nameList == null || nameList.isEmpty() || typeList == null || typeList.isEmpty()) {
            throw getErrorReporter().errorBadResultSignature();
        }
        int count = nameList.size();
        List<ADBColumn> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String columnName = nameList.get(i);
            String typeName = typeList.get(i);
            boolean optional = false;
            if (typeName.endsWith(OPTIONAL_TYPE_SUFFIX)) {
                optional = true;
                typeName = typeName.substring(0, typeName.length() - OPTIONAL_TYPE_SUFFIX.length());
            }
            ADBDatatype columnType = ADBDatatype.findByTypeName(typeName);
            if (columnType == null) {
                throw getErrorReporter().errorBadResultSignature();
            }
            result.add(new ADBColumn(columnName, columnType, optional));
        }
        return result;
    }

    public boolean isExplainOnly(QueryServiceResponse response) {
        return response.plans != null && Boolean.TRUE.equals(response.plans.explainOnly);
    }

    public int getStatementParameterCount(QueryServiceResponse response) throws SQLException {
        QueryServiceResponse.Plans plans = response.plans;
        if (plans == null) {
            throw getErrorReporter().errorInProtocol();
        }
        if (plans.statementParameters == null) {
            return 0;
        }
        int paramPos = 0;
        for (Object param : plans.statementParameters) {
            if (param instanceof Number) {
                paramPos = Math.max(paramPos, ((Number) param).intValue());
            } else {
                throw getErrorReporter().errorParameterNotSupported(String.valueOf(param));
            }
        }
        return paramPos;
    }

    public ADBProductVersion parseDatabaseVersion(String serverVersion) {
        String dbProductName = null;
        String dbProductVersion = null;
        int dbMajorVersion = 0;
        int dbMinorVersion = 0;
        if (serverVersion != null) {
            Matcher m = DATABASE_VERSION_PATTERN.matcher(serverVersion);
            if (m.matches()) {
                dbProductName = m.group("name");
                dbProductVersion = m.group("ver");
                String vermj = m.group("vermj");
                String vermn = m.group("vermn");
                if (vermj != null) {
                    try {
                        dbMajorVersion = Integer.parseInt(vermj);
                    } catch (NumberFormatException e) {
                        // ignore (overflow)
                    }
                }
                if (vermn != null) {
                    try {
                        dbMinorVersion = Integer.parseInt(vermn);
                    } catch (NumberFormatException e) {
                        // ignore (overflow)
                    }
                }
            }
        }
        return new ADBProductVersion(dbProductName, dbProductVersion, dbMajorVersion, dbMinorVersion);
    }

    public String getDefaultDataverse() {
        return DEFAULT_DATAVERSE;
    }

    public static class SubmitStatementOptions {
        public String dataverseName;
        public int timeoutSeconds;
        public boolean forceReadOnly;
        public boolean compileOnly;
        public boolean sqlCompatMode;
    }

    public static class QueryServiceResponse {

        public Status status;
        public Plans plans;
        public Signature signature;
        public String handle;
        public List<?> results; // currently only used for EXPLAIN results
        public List<Message> errors;
        public List<Message> warnings;

        public enum Status {
            RUNNING,
            SUCCESS,
            TIMEOUT,
            FAILED,
            FATAL
        }

        public enum StatementCategory {
            QUERY,
            UPDATE,
            DDL,
            PROCEDURE
        }

        public static class Signature {
            List<String> name;
            List<String> type;
        }

        public static class Plans {
            StatementCategory statementCategory;
            List<Object> statementParameters;
            Boolean explainOnly;
        }

        public static class Message {
            int code;
            String msg;
        }
    }
}
