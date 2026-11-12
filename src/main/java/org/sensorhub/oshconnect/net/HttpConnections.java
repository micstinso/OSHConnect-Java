package org.sensorhub.oshconnect.net;

import org.sensorhub.oshconnect.net.tls.TlsSettings;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Opens OSHConnect's HTTP connections: with {@link TlsSettings}, and with the
 * credentials sent on the connection itself, never through the process-wide
 * {@code java.net.Authenticator}, which would hand them to any server that asks.
 */
public final class HttpConnections {
    private HttpConnections() {
    }

    /** "Basic ..." for {@code user}, or null without a user. */
    public static String basicAuthorization(String user, char[] password) {
        if (user == null || user.isEmpty()) return null;
        String credentials = user + ":" + (password != null ? new String(password) : "");
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    public static HttpURLConnection open(URI uri, String method, String authorization) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        TlsSettings.apply(connection);
        connection.setRequestMethod(method);
        if (authorization != null) connection.setRequestProperty("Authorization", authorization);
        return connection;
    }
}
