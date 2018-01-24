package org.apache.hyracks.http.server.authenticator;

import io.netty.handler.codec.http.HttpRequest;
import org.apache.hyracks.http.api.IAuthenticator;

import java.util.Base64;

public class SimpleAuthenticator implements IAuthenticator {

    public static final String SUFFIX_BASIC_AUTHENTICATION = "Basic ";

    private String secret;
    private String userName;
    private String password;

    public SimpleAuthenticator(String userName, String password) {
        this.userName = userName;
        this.password = password;
        this.secret = SUFFIX_BASIC_AUTHENTICATION
                + Base64.getEncoder().encodeToString((userName + ":" + password).getBytes());

    }

    @Override
    public boolean validate(HttpRequest request) {
        String authorizationInfo = request.headers().get(KEY_REQUEST_AUTHORIZATION);
        return (authorizationInfo != null && authorizationInfo.equals(this.secret));
    }
}
