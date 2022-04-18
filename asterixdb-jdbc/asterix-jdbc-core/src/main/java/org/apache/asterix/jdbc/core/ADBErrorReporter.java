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

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ADBErrorReporter {

    public SQLException errorObjectClosed(Class<?> jdbcInterface) {
        return new SQLException(String.format("%s is closed", jdbcInterface.getSimpleName()));
    }

    public SQLException errorObjectClosed(Class<?> jdbcInterface, SQLState sqlState) {
        return new SQLException(String.format("%s is closed", jdbcInterface.getSimpleName()), sqlState.code);
    }

    public SQLFeatureNotSupportedException errorMethodNotSupported(Class<?> jdbcInterface, String methodName) {
        return new SQLFeatureNotSupportedException(
                String.format("Method %s.%s() is not supported", jdbcInterface.getName(), methodName));
    }

    public SQLClientInfoException errorClientInfoMethodNotSupported(Class<?> jdbcInterface, String methodName) {
        return new SQLClientInfoException(
                String.format("Method %s.%s() is not supported", jdbcInterface.getName(), methodName),
                Collections.emptyMap());
    }

    public SQLException errorParameterNotSupported(String parameterName) {
        return new SQLException(String.format("Unsupported parameter %s", parameterName));
    }

    public String warningParameterNotSupported(String parameterName) {
        return String.format("Unsupported parameter %s", parameterName);
    }

    public SQLException errorParameterValueNotSupported(String parameterName) {
        return new SQLException(String.format("Unsupported or invalid value of %s parameter", parameterName));
    }

    public String warningParameterValueNotSupported(String parameterName) {
        return String.format("Ignored unsupported or invalid value of %s parameter", parameterName);
    }

    public SQLException errorUnexpectedDriverVersion(ADBProductVersion version, ADBProductVersion minExpectedVersion) {
        return new SQLException(
                String.format("Unexpected driver version %s. Expected at least %s.%s", version.getProductVersion(),
                        minExpectedVersion.getMajorVersion(), minExpectedVersion.getMinorVersion()));
    }

    public SQLException errorUnexpectedDatabaseVersion(ADBProductVersion version,
            ADBProductVersion minExpectedVersion) {
        return new SQLException(
                String.format("Unexpected database version %s. Expected at least %s.%s", version.getProductVersion(),
                        minExpectedVersion.getMajorVersion(), minExpectedVersion.getMinorVersion()));
    }

    public SQLException errorIncompatibleMode(String mode) {
        return new SQLException(String.format("Operation cannot be performed in %s mode", mode));
    }

    public SQLException errorInProtocol() {
        return new SQLNonTransientConnectionException("Protocol error", SQLState.CONNECTION_FAILURE.code);
    }

    public SQLException errorInProtocol(String badValue) {
        return new SQLNonTransientConnectionException(String.format("Protocol error. Unexpected %s", badValue),
                SQLState.CONNECTION_FAILURE.code);
    }

    public SQLException errorInProtocol(JsonProcessingException e) {
        return new SQLNonTransientConnectionException(String.format("Protocol error. %s", getMessage(e)),
                SQLState.CONNECTION_FAILURE.code, e);
    }

    public SQLException errorInConnection(String badValue) {
        return new SQLNonTransientConnectionException(String.format("Connection error. Unexpected %s", badValue),
                SQLState.CONNECTION_FAILURE.code);
    }

    public SQLException errorInConnection(IOException e) {
        String message = String.format("Connection error. %s", getMessage(e));
        return isTimeoutConnectionError(e) ? errorTimeout(message, e)
                : isTransientConnectionError(e)
                        ? new SQLTransientConnectionException(message, SQLState.CONNECTION_FAILURE.code, e)
                        : new SQLNonTransientConnectionException(message, SQLState.CONNECTION_FAILURE.code, e);
    }

    public SQLException errorClosingResource(IOException e) {
        return new SQLException(String.format("Error closing resources. %s", getMessage(e)), e);
    }

    public SQLInvalidAuthorizationSpecException errorAuth() {
        return new SQLInvalidAuthorizationSpecException("Authentication/authorization error",
                SQLState.INVALID_AUTH_SPEC.code);
    }

    public SQLException errorColumnNotFound(String columnNameOrNumber) {
        return new SQLException(String.format("Column %s was not found", columnNameOrNumber));
    }

    public SQLException errorUnexpectedColumnValue(ADBDatatype type, String columnName) {
        return new SQLException(
                String.format("Unexpected value of type %s for column %s", type.getTypeName(), columnName));
    }

    public SQLException errorUnwrapTypeMismatch(Class<?> iface) {
        return new SQLException(String.format("Cannot unwrap to %s", iface.getName()));
    }

    public SQLException errorInvalidStatementCategory() {
        return new SQLException("Invalid statement category");
    }

    public SQLException errorUnexpectedType(Class<?> type) {
        return new SQLException(String.format("Unexpected type %s", type.getName()), SQLState.INVALID_DATE_TYPE.code);
    }

    public SQLException errorUnexpectedType(byte typeTag) {
        return new SQLException(String.format("Unexpected type %s", typeTag), SQLState.INVALID_DATE_TYPE.code);
    }

    public SQLException errorUnexpectedType(ADBDatatype type) {
        return new SQLException(String.format("Unexpected type %s", type.getTypeName()),
                SQLState.INVALID_DATE_TYPE.code);
    }

    public SQLException errorInvalidValueOfType(ADBDatatype type) {
        return new SQLException(String.format("Invalid value of type %s", type), SQLState.INVALID_DATE_TYPE.code);
    }

    public SQLException errorNoResult() {
        return new SQLException("Result is unavailable");
    }

    public SQLException errorBadResultSignature() {
        return new SQLException("Cannot infer result columns");
    }

    public SQLException errorNoCurrentRow() {
        return new SQLException("No current row", SQLState.INVALID_CURSOR_POSITION.code);
    }

    public SQLException errorInRequestGeneration(IOException e) {
        return new SQLException(String.format("Cannot create request. %s", getMessage(e)), e);
    }

    public SQLException errorInRequestURIGeneration(URISyntaxException e) {
        return new SQLException(String.format("Cannot create request URI. %s", getMessage(e)), e);
    }

    public SQLException errorInResultHandling(IOException e) {
        return new SQLException(String.format("Cannot reading result. %s", getMessage(e)), e);
    }

    public SQLTimeoutException errorTimeout() {
        return new SQLTimeoutException();
    }

    public SQLTimeoutException errorTimeout(String message, IOException cause) {
        return new SQLTimeoutException(message, cause);
    }

    protected boolean isTimeoutConnectionError(IOException e) {
        return false;
    }

    protected boolean isTransientConnectionError(IOException e) {
        return false;
    }

    protected boolean isInstanceOf(IOException e, List<Class<? extends IOException>> classList) {
        if (e != null) {
            for (Class<? extends IOException> c : classList) {
                if (c.isInstance(e)) {
                    return true;
                }
            }

        }
        return false;
    }

    public String getMessage(Exception e) {
        String message = e != null ? e.getMessage() : null;
        return message != null ? message : "";
    }

    public enum SQLState {
        CONNECTION_FAILURE("08001"), // TODO:08006??
        CONNECTION_CLOSED("08003"),
        INVALID_AUTH_SPEC("28000"),
        INVALID_DATE_TYPE("HY004"),
        INVALID_CURSOR_POSITION("HY108");

        private final String code;

        SQLState(String code) {
            this.code = Objects.requireNonNull(code);
        }

        @Override
        public String toString() {
            return code;
        }
    }
}
