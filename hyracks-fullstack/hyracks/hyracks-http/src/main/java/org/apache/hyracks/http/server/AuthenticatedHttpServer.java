package org.apache.hyracks.http.server;

import org.apache.hyracks.http.api.IAuthenticator;

import io.netty.channel.EventLoopGroup;

public class AuthenticatedHttpServer extends HttpServer {

    private IAuthenticator authenticator;

    public AuthenticatedHttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port, IAuthenticator authenticator) {
        super(bossGroup, workerGroup, port);
        this.authenticator = authenticator;
    }

    @Override
    protected HttpServerHandler<? extends HttpServer> createHttpHandler(int chunkSize) {
        return new AuthenticatedHttpServerHandler(this, chunkSize, authenticator);
    }

}
