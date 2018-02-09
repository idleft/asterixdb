/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.hyracks.http.server.authenticator;

import io.netty.handler.codec.http.HttpRequest;
import org.apache.hyracks.http.api.IAuthenticator;

import java.util.Base64;

public class BasicAuthenticator implements IAuthenticator {

    public static final String SUFFIX_BASIC_AUTHENTICATION = "Basic ";

    private String secret;

    public BasicAuthenticator(String username, String password) {
        this.secret = SUFFIX_BASIC_AUTHENTICATION
                + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());

    }

    @Override
    public boolean validate(HttpRequest request) {
        String authorizationInfo = request.headers().get(KEY_REQUEST_AUTHORIZATION);
        return authorizationInfo != null && authorizationInfo.equals(this.secret);
    }
}
