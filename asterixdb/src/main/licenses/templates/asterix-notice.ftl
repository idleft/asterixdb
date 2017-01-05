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
Apache AsterixDB ${packageName}
Copyright 2015-2017 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).

AsterixDB utilizes many libraries, which come with the following applicable NOTICE(s):

<#list noticeMap as e>
   <#assign noticeText = e.getKey()/>
   <#assign projects = e.getValue()/>
   <#list projects as p>
       <#list p.locations as loc>
- ${loc}${p.artifactId}-${p.version}.jar
       </#list>
   </#list>

<@indent spaces=6>
${noticeText}
</@indent>

</#list>
