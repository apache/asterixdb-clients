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

public class ADBProductVersion {

    public static final String ASTERIXDB = "Apache AsterixDB";

    private final String productName;

    private final String productVersion;

    private final int majorVersion;

    private final int minorVersion;

    public ADBProductVersion(String productName, String productVersion, int majorVersion, int minorVersion) {
        this.productName = productName != null ? productName : ASTERIXDB;
        this.productVersion = productVersion != null ? productVersion : majorVersion + "." + minorVersion;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public String getProductName() {
        return productName;
    }

    public String getProductVersion() {
        return productVersion;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public boolean isAtLeast(ADBProductVersion otherVersion) {
        return majorVersion == otherVersion.majorVersion ? minorVersion >= otherVersion.minorVersion
                : majorVersion > otherVersion.majorVersion;
    }

    @Override
    public String toString() {
        return productName + '/' + productVersion;
    }
}
