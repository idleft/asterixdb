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
