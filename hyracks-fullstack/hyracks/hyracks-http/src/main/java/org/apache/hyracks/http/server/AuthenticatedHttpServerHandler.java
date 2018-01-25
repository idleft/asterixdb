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
package org.apache.hyracks.http.server;

import java.io.IOException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hyracks.http.api.IAuthenticator;
import org.apache.hyracks.http.api.IServlet;

public class AuthenticatedHttpServerHandler extends HttpServerHandler {

    private IAuthenticator authenticator;

    public AuthenticatedHttpServerHandler(HttpServer server, int chunkSize, IAuthenticator authenticator) {
        super(server, chunkSize);
        this.authenticator = authenticator;
    }

    @Override
    protected void submit(ChannelHandlerContext ctx, IServlet servlet, FullHttpRequest request) throws IOException {
        if (!authenticator.validate(request)) {
            respond(ctx, request.protocolVersion(), HttpResponseStatus.UNAUTHORIZED);
        } else {
            super.submit(ctx, servlet, request);
        }
    }
}
