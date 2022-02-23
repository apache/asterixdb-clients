<#--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
-->
<#-- TODO(mblow): share notice file template with hyracks via maven artifact -->
<#if packageName?has_content>
${packageName!}
<#else>
Apache AsterixDB JDBC Driver
</#if>
Copyright 2021-${.now?string('yyyy')} The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).

The Initial Developer of the driver software is Couchbase, Inc.
Copyright 2021 Couchbase, Inc.
<#list noticeMap>

AsterixDB JDBC Driver utilizes several libraries, which come with the following applicable NOTICE(s):
<#items as e>
   <#assign noticeText = e.getKey()/>
   <#assign projects = e.getValue()/>

   <#list projects as p>
   * ${p.name} (${p.groupId}:${p.artifactId}:${p.version})
   </#list>

<@indent spaces=6>
${noticeText}
</@indent>
</#items>
</#list>
