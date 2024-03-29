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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ADBConnection extends ADBWrapperSupport implements Connection {

    protected final ADBProtocolBase protocol;
    protected final String url;
    protected final ADBProductVersion databaseVersion;
    protected final ADBDriverProperty.CatalogDataverseMode catalogDataverseMode;
    protected final boolean catalogIncludesSchemaless;
    protected final boolean sqlCompatMode;
    private final AtomicBoolean closed;
    private final ConcurrentLinkedQueue<ADBStatement> statements;
    private volatile SQLWarning warning;
    private volatile ADBMetaStatement metaStatement;
    private volatile String catalog;
    private volatile String schema;
    private final boolean databaseEntitySupported;

    // Lifecycle

    public ADBConnection(ADBProtocolBase protocol, String url, ADBProductVersion databaseVersion,
            String dataverseCanonicalName, Map<ADBDriverProperty, Object> properties, SQLWarning connectWarning)
            throws SQLException {
        this.url = Objects.requireNonNull(url);
        this.protocol = Objects.requireNonNull(protocol);
        this.databaseVersion = databaseVersion;
        this.statements = new ConcurrentLinkedQueue<>();
        this.warning = connectWarning;
        this.closed = new AtomicBoolean(false);
        this.sqlCompatMode = (Boolean) ADBDriverProperty.Common.SQL_COMPAT_MODE.fetchPropertyValue(properties);
        this.catalogDataverseMode = getCatalogDataverseMode(properties, protocol.getErrorReporter());
        this.catalogIncludesSchemaless =
                (Boolean) ADBDriverProperty.Common.CATALOG_INCLUDES_SCHEMALESS.fetchPropertyValue(properties);
        this.databaseEntitySupported = checkDatabaseEntitySupport();
        initCatalogSchema(protocol, dataverseCanonicalName);
    }

    protected void initCatalogSchema(ADBProtocolBase protocol, String dataverseCanonicalName) throws SQLException {
        switch (catalogDataverseMode) {
            case CATALOG:
                if (dataverseCanonicalName == null || dataverseCanonicalName.isEmpty()) {
                    catalog = isDatabaseEntitySupported()
                            ? protocol.getDefaultDatabase() + "/" + protocol.getDefaultDataverse()
                            : protocol.getDefaultDataverse();
                } else {
                    catalog = dataverseCanonicalName;
                }
                // schema = null
                break;
            case CATALOG_SCHEMA:
                if (dataverseCanonicalName == null || dataverseCanonicalName.isEmpty()) {
                    if (isDatabaseEntitySupported()) {
                        catalog = protocol.getDefaultDatabase();
                        schema = protocol.getDefaultDataverse();
                    } else {
                        catalog = protocol.getDefaultDataverse();
                        // schema = null
                    }
                } else {
                    String[] parts = dataverseCanonicalName.split("/");
                    switch (parts.length) {
                        case 1:
                            catalog = parts[0];
                            break;
                        case 2:
                            catalog = parts[0];
                            schema = parts[1];
                            break;
                        default:
                            throw protocol.getErrorReporter().errorInConnection(dataverseCanonicalName); //TODO:FIXME
                    }
                }
                break;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void close() throws SQLException {
        closeImpl(null);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw getErrorReporter().errorParameterValueNotSupported("executor");
        }
        SecurityManager sec = System.getSecurityManager();
        if (sec != null) {
            sec.checkPermission(new SQLPermission("callAbort"));
        }
        closeImpl(executor);
    }

    protected void closeImpl(Executor executor) throws SQLException {
        boolean wasClosed = closed.getAndSet(true);
        if (wasClosed) {
            return;
        }
        if (executor == null) {
            closeStatementsAndProtocol();
        } else {
            executor.execute(() -> {
                try {
                    closeStatementsAndProtocol();
                } catch (SQLException e) {
                    if (getLogger().isLoggable(Level.FINE)) {
                        getLogger().log(Level.FINE, e.getMessage(), e);
                    }
                }
            });
        }
    }

    protected void closeStatementsAndProtocol() throws SQLException {
        SQLException err = null;
        try {
            closeRegisteredStatements();
        } catch (SQLException e) {
            err = e;
        }
        try {
            protocol.close();
        } catch (SQLException e) {
            if (err != null) {
                e.addSuppressed(err);
            }
            err = e;
        }
        if (err != null) {
            throw err;
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw getErrorReporter().errorObjectClosed(Connection.class, ADBErrorReporter.SQLState.CONNECTION_CLOSED);
        }
    }

    // Connectivity

    @Override
    public boolean isValid(int timeoutSeconds) throws SQLException {
        if (isClosed()) {
            return false;
        }
        if (timeoutSeconds < 0) {
            throw getErrorReporter().errorParameterValueNotSupported("timeoutSeconds");
        }
        return protocol.ping(timeoutSeconds);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setNetworkTimeout");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "getNetworkTimeout");
    }

    // Metadata

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        ADBMetaStatement metaStatement = getOrCreateMetaStatement();
        return createDatabaseMetaData(metaStatement);
    }

    private ADBMetaStatement getOrCreateMetaStatement() {
        ADBMetaStatement stmt = metaStatement;
        if (stmt == null) {
            synchronized (this) {
                stmt = metaStatement;
                if (stmt == null) {
                    stmt = createMetaStatement();
                    registerStatement(stmt);
                    metaStatement = stmt;
                }
            }
        }
        return stmt;
    }

    protected ADBMetaStatement createMetaStatement() {
        return new ADBMetaStatement(this);
    }

    protected ADBDatabaseMetaData createDatabaseMetaData(ADBMetaStatement metaStatement) {
        return new ADBDatabaseMetaData(metaStatement, databaseVersion);
    }

    // Statement construction

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return createStatementImpl();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        checkResultSetConfig(resultSetType, resultSetConcurrency, resultSetHoldability);
        return createStatementImpl();
    }

    private void checkResultSetConfig(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        boolean ok = resultSetType == ResultSet.TYPE_FORWARD_ONLY && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY;
        if (!ok) {
            throw getErrorReporter().errorParameterValueNotSupported("resultSetType/resultSetConcurrency");
        }
        if (resultSetHoldability != ADBResultSet.RESULT_SET_HOLDABILITY) {
            if (getLogger().isLoggable(Level.FINE)) {
                getLogger().log(Level.FINE,
                        getErrorReporter().warningParameterValueNotSupported("ResultSetHoldability"));
            }
        }
    }

    protected ADBStatement createStatementImpl() {
        ADBStatement stmt = new ADBStatement(this);
        registerStatement(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return prepareStatementImpl(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkClosed();
        checkResultSetConfig(resultSetType, resultSetConcurrency, resultSetHoldability);
        return prepareStatementImpl(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareStatement");
    }

    private ADBPreparedStatement prepareStatementImpl(String sql) throws SQLException {
        ADBPreparedStatement stmt = new ADBPreparedStatement(this, sql);
        registerStatement(stmt);
        return stmt;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareCall");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareCall");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareCall");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return schema;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        if (catalogDataverseMode == ADBDriverProperty.CatalogDataverseMode.CATALOG
                && (schema != null && !schema.isEmpty())) {
            throw getErrorReporter().errorInConnection(schema); //TODO:FIXME:REVIEW make no-op?
        }
        this.schema = schema;
    }

    protected String getDataverseCanonicalName() {
        switch (catalogDataverseMode) {
            case CATALOG:
                return catalog;
            case CATALOG_SCHEMA:
                String c = catalog;
                String s = schema;
                return s == null ? c : c + "/" + s;
            default:
                throw new IllegalStateException();
        }
    }

    protected static ADBDriverProperty.CatalogDataverseMode getCatalogDataverseMode(
            Map<ADBDriverProperty, Object> properties, ADBErrorReporter errorReporter) throws SQLException {
        int mode = ((Number) ADBDriverProperty.Common.CATALOG_DATAVERSE_MODE.fetchPropertyValue(properties)).intValue();
        try {
            return ADBDriverProperty.CatalogDataverseMode.valueOf(mode);
        } catch (IllegalArgumentException e) {
            throw errorReporter.errorInConnection(String.valueOf(mode)); //TODO:FIXME
        }
    }

    // Statement lifecycle

    private void registerStatement(ADBStatement stmt) {
        statements.add(Objects.requireNonNull(stmt));
    }

    void deregisterStatement(ADBStatement stmt) {
        statements.remove(Objects.requireNonNull(stmt));
    }

    private void closeRegisteredStatements() throws SQLException {
        SQLException err = null;
        ADBStatement statement;
        while ((statement = statements.poll()) != null) {
            try {
                statement.closeImpl(true, false);
            } catch (SQLException e) {
                if (err != null) {
                    e.addSuppressed(err);
                }
                err = e;
            }
        }
        if (err != null) {
            throw err;
        }
    }

    // Transaction control

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        switch (level) {
            case Connection.TRANSACTION_READ_COMMITTED:
                break;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().log(Level.FINE,
                            getErrorReporter().warningParameterValueNotSupported("TransactionIsolationLevel"));
                }
                break;
            default:
                throw getErrorReporter().errorParameterValueNotSupported("TransactionIsolationLevel");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        switch (holdability) {
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                break;
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().log(Level.FINE, getErrorReporter().warningParameterValueNotSupported("Holdability"));
                }
                break;
            default:
                throw getErrorReporter().errorParameterValueNotSupported("Holdability");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("AutoCommit");
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("AutoCommit");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setSavepoint");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "releaseSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "rollback");
    }

    // Value construction

    @Override
    public Clob createClob() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createClob");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createBlob");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createNClob");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createStruct");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createSQLXML");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return Collections.emptyMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setTypeMap");
    }

    // Unsupported hints (ignored)

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
    }

    // Errors and warnings

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return warning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        warning = null;
    }

    @Override
    protected ADBErrorReporter getErrorReporter() {
        return protocol.getErrorReporter();
    }

    protected Logger getLogger() {
        return protocol.getLogger();
    }

    // Miscellaneous unsupported features (error is raised)

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return new Properties();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw getErrorReporter().errorClientInfoMethodNotSupported(Connection.class, "setClientInfo");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw getErrorReporter().errorClientInfoMethodNotSupported(Connection.class, "setClientInfo");
    }

    protected boolean checkDatabaseEntitySupport() throws SQLException {
        checkClosed();

        StringBuilder sql = new StringBuilder(256);
        ADBStatement stmt = createStatementImpl();

        sql.append("select count(*) ");
        sql.append("from Metadata.`Dataset` ");
        sql.append("where DataverseName='Metadata' and DatasetName='Database'");
        ADBResultSet resultSet = stmt.executeQuery(sql.toString());
        try {
            if (resultSet.next()) {
                return resultSet.getInt(1) > 0;
            }
            return false;
        } finally {
            stmt.close();
        }
    }

    public boolean isDatabaseEntitySupported() {
        return databaseEntitySupported;
    }
}
