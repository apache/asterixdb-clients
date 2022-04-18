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

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JdbcDriverTest {

    static final String ASTERIX_APP_PATH_PROPERTY = "asterix-app.dir";

    static final String ASTERIX_APP_PATH = System.getProperty(ASTERIX_APP_PATH_PROPERTY);

    static final List<Class<? extends JdbcTester>> TESTER_CLASSES =
            Arrays.asList(JdbcMetadataTester.class, JdbcConnectionTester.class, JdbcStatementTester.class,
                    JdbcPreparedStatementTester.class, JdbcResultSetTester.JdbcStatementResultSetTester.class,
                    JdbcResultSetTester.JdbcPreparedStatementResultSetTester.class, JdbcStatementParameterTester.class);

    public static final String TEST_METHOD_PREFIX = "test";

    private static JdbcTester.JdbcTestContext testContext;

    private final Class<? extends JdbcTester> testerClass;

    private final Method testMethod;

    public JdbcDriverTest(String simpleClassName, String methodName) throws Exception {
        Optional<Class<? extends JdbcTester>> testerClassRef =
                TESTER_CLASSES.stream().filter(c -> c.getSimpleName().equals(simpleClassName)).findFirst();
        if (testerClassRef.isEmpty()) {
            throw new Exception("Cannot find class: " + simpleClassName);
        }
        testerClass = testerClassRef.get();
        Optional<Method> testMethodRef = Arrays.stream(testerClassRef.get().getMethods())
                .filter(m -> m.getName().equals(methodName)).findFirst();
        if (testMethodRef.isEmpty()) {
            throw new Exception("Cannot find method: " + methodName + " in class " + testerClass.getName());
        }
        testMethod = testMethodRef.get();
    }

    @Parameterized.Parameters(name = "JdbcDriverTest {index}: {0}.{1}")
    public static Collection<Object[]> tests() {
        List<Object[]> testsuite = new ArrayList<>();
        for (Class<? extends JdbcTester> testerClass : TESTER_CLASSES) {
            Arrays.stream(testerClass.getMethods()).map(Method::getName).filter(n -> n.startsWith(TEST_METHOD_PREFIX))
                    .sorted().forEach(n -> testsuite.add(new Object[] { testerClass.getSimpleName(), n }));
        }
        return testsuite;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        if (ASTERIX_APP_PATH == null) {
            throw new Exception(String.format("Property %s is not set", ASTERIX_APP_PATH_PROPERTY));
        }

        Path ccConfigFile = Path.of(ASTERIX_APP_PATH, "src", TEST_METHOD_PREFIX, "resources", "cc.conf");

        ExecutionTestUtil.setUp(true, ccConfigFile.toString(), ExecutionTestUtil.integrationUtil, false,
                Collections.emptyList());

        NodeControllerService nc = ExecutionTestUtil.integrationUtil.ncs[0];
        String host = InetAddress.getLoopbackAddress().getHostAddress();
        INcApplicationContext appCtx = (INcApplicationContext) nc.getApplicationContext();
        int apiPort = appCtx.getExternalProperties().getNcApiPort();

        testContext = JdbcTester.createTestContext(host, apiPort);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ExecutionTestUtil.tearDown(true, false);
    }

    @Test
    public void test() throws Exception {
        JdbcTester tester = testerClass.getDeclaredConstructor().newInstance();
        tester.setTestContext(testContext);
        testMethod.invoke(tester);
    }
}
