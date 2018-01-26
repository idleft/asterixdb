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

import org.apache.hyracks.http.api.IAuthenticator;

import io.netty.channel.EventLoopGroup;

public class AuthenticatedHttpServer extends HttpServer {

    private IAuthenticator authenticator;

    public AuthenticatedHttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port,
            IAuthenticator authenticator) {
        super(bossGroup, workerGroup, port);
        this.authenticator = authenticator;
    }

    @Override
    protected HttpServerHandler<? extends HttpServer> createHttpHandler(int chunkSize) {
        return new AuthenticatedHttpServerHandler(this, chunkSize, authenticator);
    }

}
