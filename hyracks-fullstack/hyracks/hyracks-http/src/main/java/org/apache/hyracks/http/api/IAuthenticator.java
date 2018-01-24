package org.apache.hyracks.http.api;

import io.netty.handler.codec.http.HttpRequest;

public interface IAuthenticator {

    public static final String KEY_REQUEST_AUTHORIZATION = "Authorization";

    public boolean validate(HttpRequest request);
}
