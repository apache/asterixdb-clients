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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;

public class ADBProtocol extends ADBProtocolBase {

    private static final String QUERY_SERVICE_ENDPOINT_PATH = "/query/service";
    private static final String QUERY_RESULT_ENDPOINT_PATH = "/query/service/result";
    private static final String ACTIVE_REQUESTS_ENDPOINT_PATH = "/admin/requests/running";

    final URI queryEndpoint;
    final URI queryResultEndpoint;
    final URI activeRequestsEndpoint;
    final HttpClientConnectionManager httpConnectionManager;
    final HttpClientContext httpClientContext;
    final CloseableHttpClient httpClient;

    public ADBProtocol(String host, int port, Map<ADBDriverProperty, Object> params, ADBDriverContext driverContext)
            throws SQLException {
        super(driverContext, params);
        URI queryEndpoint =
                createEndpointUri(host, port, QUERY_SERVICE_ENDPOINT_PATH, driverContext.getErrorReporter());
        URI queryResultEndpoint =
                createEndpointUri(host, port, QUERY_RESULT_ENDPOINT_PATH, driverContext.getErrorReporter());
        URI activeRequestsEndpoint =
                createEndpointUri(host, port, getActiveRequestsEndpointPath(params), driverContext.getErrorReporter());

        PoolingHttpClientConnectionManager httpConnectionManager = new PoolingHttpClientConnectionManager();
        int maxConnections = Math.max(16, Runtime.getRuntime().availableProcessors());
        httpConnectionManager.setDefaultMaxPerRoute(maxConnections);
        httpConnectionManager.setMaxTotal(maxConnections);
        SocketConfig.Builder socketConfigBuilder = null;
        Number socketTimeoutMillis = (Number) ADBDriverProperty.Common.SOCKET_TIMEOUT.fetchPropertyValue(params);
        if (socketTimeoutMillis != null) {
            socketConfigBuilder = SocketConfig.custom();
            socketConfigBuilder.setSoTimeout(socketTimeoutMillis.intValue());
        }
        if (socketConfigBuilder != null) {
            httpConnectionManager.setDefaultSocketConfig(socketConfigBuilder.build());
        }
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        Number connectTimeoutMillis = (Number) ADBDriverProperty.Common.CONNECT_TIMEOUT.fetchPropertyValue(params);
        if (connectTimeoutMillis != null) {
            requestConfigBuilder.setConnectionRequestTimeout(connectTimeoutMillis.intValue());
            requestConfigBuilder.setConnectTimeout(connectTimeoutMillis.intValue());
        }
        if (socketTimeoutMillis != null) {
            requestConfigBuilder.setSocketTimeout(socketTimeoutMillis.intValue());
        }
        RequestConfig requestConfig = requestConfigBuilder.build();
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setConnectionManager(httpConnectionManager);
        httpClientBuilder.setConnectionManagerShared(true);
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        if (user != null) {
            String password = (String) ADBDriverProperty.Common.PASSWORD.fetchPropertyValue(params);
            httpClientBuilder.setDefaultCredentialsProvider(createCredentialsProvider(user, password));
        }
        this.queryEndpoint = queryEndpoint;
        this.queryResultEndpoint = queryResultEndpoint;
        this.activeRequestsEndpoint = activeRequestsEndpoint;
        this.httpConnectionManager = httpConnectionManager;
        this.httpClient = httpClientBuilder.build();
        this.httpClientContext = createHttpClientContext(queryEndpoint);
    }

    @Override
    public void close() throws SQLException {
        try {
            httpClient.close();
        } catch (IOException e) {
            throw getErrorReporter().errorClosingResource(e);
        } finally {
            httpConnectionManager.shutdown();
        }
    }

    @Override
    public String connect() throws SQLException {
        String databaseVersion = pingImpl(-1, true); // TODO:review timeout
        if (getLogger().isLoggable(Level.FINE)) {
            getLogger().log(Level.FINE, String.format("connected to '%s' at %s", databaseVersion, queryEndpoint));
        }
        return databaseVersion;
    }

    @Override
    public boolean ping(int timeoutSeconds) {
        try {
            pingImpl(timeoutSeconds, false);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private String pingImpl(int timeoutSeconds, boolean fetchDatabaseVersion) throws SQLException {
        //TODO: support timeoutSeconds: -1 = use default, 0 = indefinite ?
        HttpOptions httpOptions = new HttpOptions(queryEndpoint);
        try (CloseableHttpResponse response = httpClient.execute(httpOptions, httpClientContext)) {
            int statusCode = response.getStatusLine().getStatusCode();
            switch (statusCode) {
                case HttpStatus.SC_OK:
                    String databaseVersion = null;
                    if (fetchDatabaseVersion) {
                        Header serverHeader = response.getFirstHeader(HttpHeaders.SERVER);
                        if (serverHeader != null) {
                            databaseVersion = serverHeader.getValue();
                        }
                    }
                    return databaseVersion;
                case HttpStatus.SC_UNAUTHORIZED:
                case HttpStatus.SC_FORBIDDEN:
                    throw getErrorReporter().errorAuth();
                default:
                    throw getErrorReporter().errorInConnection(String.valueOf(response.getStatusLine()));
            }
        } catch (IOException e) {
            throw getErrorReporter().errorInConnection(e);
        }
    }

    @Override
    public QueryServiceResponse submitStatement(String sql, List<?> args, UUID executionId,
            SubmitStatementOptions options) throws SQLException {
        HttpPost httpPost = new HttpPost(queryEndpoint);
        httpPost.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON
                .withParameters(new BasicNameValuePair(FORMAT_LOSSLESS_ADM, Boolean.TRUE.toString())).toString());

        ByteArrayOutputStreamImpl baos = new ByteArrayOutputStreamImpl(512);
        try {
            JsonGenerator jsonGen =
                    driverContext.getGenericObjectWriter().getFactory().createGenerator(baos, JsonEncoding.UTF8);
            jsonGen.writeStartObject();
            jsonGen.writeStringField(CLIENT_TYPE, CLIENT_TYPE_JDBC);
            jsonGen.writeStringField(MODE, MODE_DEFERRED);
            jsonGen.writeStringField(STATEMENT, sql);
            jsonGen.writeBooleanField(SIGNATURE, true);
            jsonGen.writeStringField(PLAN_FORMAT, PLAN_FORMAT_STRING);
            jsonGen.writeNumberField(MAX_WARNINGS, maxWarnings);
            if (options.compileOnly) {
                jsonGen.writeBooleanField(COMPILE_ONLY, true);
            }
            if (options.forceReadOnly) {
                jsonGen.writeBooleanField(READ_ONLY, true);
            }
            if (options.sqlCompatMode) {
                jsonGen.writeBooleanField(SQL_COMPAT, true);
            }
            if (options.timeoutSeconds > 0) {
                jsonGen.writeStringField(TIMEOUT, options.timeoutSeconds + "s");
            }
            if (options.dataverseName != null) {
                jsonGen.writeStringField(DATAVERSE, options.dataverseName);
            }
            if (executionId != null) {
                jsonGen.writeStringField(CLIENT_CONTEXT_ID, executionId.toString());
            }
            if (args != null && !args.isEmpty()) {
                jsonGen.writeFieldName(ARGS);
                driverContext.getAdmFormatObjectWriter().writeValue(jsonGen, args);
            }
            jsonGen.writeEndObject();
            jsonGen.flush();
        } catch (InvalidDefinitionException e) {
            throw getErrorReporter().errorUnexpectedType(e.getType().getRawClass());
        } catch (IOException e) {
            throw getErrorReporter().errorInRequestGeneration(e);
        }

        if (getLogger().isLoggable(Level.FINE)) {
            getLogger().log(Level.FINE, String.format("%s { %s } with args { %s }",
                    options.compileOnly ? "compile" : "execute", sql, args != null ? args : ""));
        }

        httpPost.setEntity(new EntityTemplateImpl(baos, ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost, httpClientContext)) {
            return handlePostQueryResponse(httpResponse);
        } catch (JsonProcessingException e) {
            throw getErrorReporter().errorInProtocol(e);
        } catch (IOException e) {
            throw getErrorReporter().errorInConnection(e);
        }
    }

    private QueryServiceResponse handlePostQueryResponse(CloseableHttpResponse httpResponse)
            throws SQLException, IOException {
        int httpStatus = httpResponse.getStatusLine().getStatusCode();
        switch (httpStatus) {
            case HttpStatus.SC_OK:
            case HttpStatus.SC_BAD_REQUEST:
            case HttpStatus.SC_INTERNAL_SERVER_ERROR:
            case HttpStatus.SC_SERVICE_UNAVAILABLE:
                break;
            case HttpStatus.SC_UNAUTHORIZED:
            case HttpStatus.SC_FORBIDDEN:
                throw getErrorReporter().errorAuth();
            default:
                throw getErrorReporter().errorInProtocol(httpResponse.getStatusLine().toString());
        }
        QueryServiceResponse response;
        try (InputStream contentStream = httpResponse.getEntity().getContent()) {
            response =
                    driverContext.getGenericObjectReader().forType(QueryServiceResponse.class).readValue(contentStream);
        }
        QueryServiceResponse.Status status = response.status;
        if (httpStatus == HttpStatus.SC_OK && status == QueryServiceResponse.Status.SUCCESS) {
            return response;
        }
        if (status == QueryServiceResponse.Status.TIMEOUT) {
            throw getErrorReporter().errorTimeout();
        }
        SQLException exc = getErrorIfExists(response);
        if (exc != null) {
            throw exc;
        } else {
            throw getErrorReporter().errorInProtocol(httpResponse.getStatusLine().toString());
        }
    }

    @Override
    public JsonParser fetchResult(QueryServiceResponse response) throws SQLException {
        if (response.handle == null) {
            throw getErrorReporter().errorInProtocol();
        }
        int p = response.handle.lastIndexOf("/");
        if (p < 0) {
            throw getErrorReporter().errorInProtocol(response.handle);
        }
        String handlePath = response.handle.substring(p);
        URI resultRequestURI;
        try {
            resultRequestURI = new URI(queryResultEndpoint + handlePath);
        } catch (URISyntaxException e) {
            throw getErrorReporter().errorInProtocol(handlePath);
        }
        HttpGet httpGet = new HttpGet(resultRequestURI);
        httpGet.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());

        CloseableHttpResponse httpResponse = null;
        InputStream httpContentStream = null;
        JsonParser parser = null;
        try {
            httpResponse = httpClient.execute(httpGet, httpClientContext);
            int httpStatus = httpResponse.getStatusLine().getStatusCode();
            if (httpStatus != HttpStatus.SC_OK) {
                throw getErrorReporter().errorNoResult();
            }
            HttpEntity entity = httpResponse.getEntity();
            httpContentStream = entity.getContent();
            parser = driverContext.getGenericObjectReader().getFactory()
                    .createParser(new InputStreamWithAttachedResource(httpContentStream, httpResponse));
            if (!advanceToArrayField(parser, RESULTS)) {
                throw getErrorReporter().errorInProtocol();
            }
            parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
            return parser;
        } catch (SQLException e) {
            closeQuietly(e, parser, httpContentStream, httpResponse);
            throw e;
        } catch (JsonProcessingException e) {
            closeQuietly(e, parser, httpContentStream, httpResponse);
            throw getErrorReporter().errorInProtocol(e);
        } catch (IOException e) {
            closeQuietly(e, parser, httpContentStream, httpResponse);
            throw getErrorReporter().errorInConnection(e);
        }
    }

    private boolean advanceToArrayField(JsonParser parser, String fieldName) throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) {
            return false;
        }
        for (;;) {
            JsonToken token = parser.nextValue();
            if (token == null || token == JsonToken.END_OBJECT) {
                return false;
            }
            if (parser.currentName().equals(fieldName)) {
                return token == JsonToken.START_ARRAY;
            } else if (token.isStructStart()) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }
    }

    @Override
    public void cancelRunningStatement(UUID executionId) throws SQLException {
        HttpDelete httpDelete;
        try {
            URIBuilder uriBuilder = new URIBuilder(activeRequestsEndpoint);
            uriBuilder.setParameter(CLIENT_CONTEXT_ID, String.valueOf(executionId));
            httpDelete = new HttpDelete(uriBuilder.build());
        } catch (URISyntaxException e) {
            throw getErrorReporter().errorInRequestURIGeneration(e);
        }

        try (CloseableHttpResponse httpResponse = httpClient.execute(httpDelete, httpClientContext)) {
            int httpStatus = httpResponse.getStatusLine().getStatusCode();
            switch (httpStatus) {
                case HttpStatus.SC_OK:
                case HttpStatus.SC_NOT_FOUND:
                    break;
                case HttpStatus.SC_UNAUTHORIZED:
                case HttpStatus.SC_FORBIDDEN:
                    throw getErrorReporter().errorAuth();
                default:
                    throw getErrorReporter().errorInProtocol(httpResponse.getStatusLine().toString());
            }
        } catch (IOException e) {
            throw getErrorReporter().errorInConnection(e);
        }
    }

    @Override
    public ADBErrorReporter getErrorReporter() {
        return driverContext.getErrorReporter();
    }

    @Override
    public Logger getLogger() {
        return driverContext.getLogger();
    }

    private static CredentialsProvider createCredentialsProvider(String user, String password) {
        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
        return cp;
    }

    private static HttpClientContext createHttpClientContext(URI uri) {
        HttpClientContext hcCtx = HttpClientContext.create();
        AuthCache ac = new BasicAuthCache();
        ac.put(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()), new BasicScheme());
        hcCtx.setAuthCache(ac);
        return hcCtx;
    }

    private static void closeQuietly(Exception mainExc, java.io.Closeable... closeableList) {
        for (Closeable closeable : closeableList) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    if (mainExc != null) {
                        mainExc.addSuppressed(e);
                    }
                }
            }
        }
    }

    private static URI createEndpointUri(String host, int port, String path, ADBErrorReporter errorReporter)
            throws SQLException {
        try {
            return new URI("http", null, host, port, path, null, null);
        } catch (URISyntaxException e) {
            throw errorReporter.errorParameterValueNotSupported("endpoint " + host + ":" + port);
        }
    }

    private String getActiveRequestsEndpointPath(Map<ADBDriverProperty, Object> params) {
        String path = (String) ADBDriverProperty.Common.ACTIVE_REQUESTS_PATH.fetchPropertyValue(params);
        return path != null ? path : ACTIVE_REQUESTS_ENDPOINT_PATH;
    }

    static final class ByteArrayOutputStreamImpl extends ByteArrayOutputStream implements ContentProducer {
        private ByteArrayOutputStreamImpl(int size) {
            super(size);
        }
    }

    static final class EntityTemplateImpl extends EntityTemplate {

        private final long contentLength;

        private EntityTemplateImpl(ByteArrayOutputStreamImpl baos, ContentType contentType) {
            super(baos);
            contentLength = baos.size();
            setContentType(contentType.toString());
        }

        @Override
        public long getContentLength() {
            return contentLength;
        }
    }

    static final class InputStreamWithAttachedResource extends FilterInputStream {

        private final Closeable resource;

        private InputStreamWithAttachedResource(InputStream delegate, Closeable resource) {
            super(delegate);
            this.resource = Objects.requireNonNull(resource);
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                resource.close();
            }
        }
    }

}
