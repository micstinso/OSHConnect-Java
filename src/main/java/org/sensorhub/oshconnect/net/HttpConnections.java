package org.sensorhub.oshconnect.net;

import org.sensorhub.oshconnect.net.tls.TlsSettings;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * Opens OSHConnect's HTTP connections: with {@link TlsSettings}, and with the
 * credentials sent on the connection itself, never through the process-wide
 * {@code java.net.Authenticator}, which would hand them to any server that asks.
 */
public final class HttpConnections {
    private HttpConnections() {
    }

    /**
     * "Basic ..." for {@code user}, or null without a user.
     *
     * <p>The plain-text "user:password" is only ever held in char and byte arrays,
     * which are zeroed before returning, never in a {@code String}. The caller's
     * {@code password} array is left as it is; clearing it is the caller's job.
     */
    public static String basicAuthorization(String user, char[] password) {
        if (user == null || user.isEmpty()) return null;
        int pwLength = password != null ? password.length : 0;
        char[] credentials = new char[user.length() + 1 + pwLength];
        ByteBuffer utf8 = null;
        byte[] raw = null;
        try {
            user.getChars(0, user.length(), credentials, 0);
            credentials[user.length()] = ':';
            if (pwLength > 0) System.arraycopy(password, 0, credentials, user.length() + 1, pwLength);
            utf8 = StandardCharsets.UTF_8.encode(CharBuffer.wrap(credentials));
            raw = new byte[utf8.remaining()];
            utf8.get(raw);
            return "Basic " + Base64.getEncoder().encodeToString(raw);
        } finally {
            Arrays.fill(credentials, '\0');
            if (raw != null) Arrays.fill(raw, (byte) 0);
            if (utf8 != null && utf8.hasArray()) Arrays.fill(utf8.array(), (byte) 0);
        }
    }

    public static HttpURLConnection open(URI uri, String method, String authorization) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        TlsSettings.apply(connection);
        connection.setRequestMethod(method);
        if (authorization != null) connection.setRequestProperty("Authorization", authorization);
        return connection;
    }
}
