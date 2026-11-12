package org.sensorhub.oshconnect.net;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.sensorhub.impl.service.consys.ResourceParseException;
import org.sensorhub.impl.service.consys.client.ConSysApiClient;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * osh-core's ConSysApiClient with every request made through
 * {@link HttpConnections}: OSHConnect's TLS settings and credentials, the same
 * way on the JVM and on Android. osh-core's own requests use the process-wide
 * {@code java.net.Authenticator} and, on Android, {@code HttpsURLConnection}'s
 * process-wide defaults, which an application can only change for everything
 * in its process.
 *
 * <p>Replaces osh-core's five request methods; responses are handled as osh-core
 * handles them.
 */
public class OshConSysApiClient extends ConSysApiClient {
    private final String authorization;

    public OshConSysApiClient(String endpoint, String user, char[] password) {
        try {
            this.endpoint = new URI(endpoint.endsWith("/") ? endpoint : endpoint + "/");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI " + endpoint);
        }
        this.authorization = HttpConnections.basicAuthorization(user, password);
    }

    private HttpURLConnection open(URI uri, String method, ResourceFormat accept, ResourceFormat contentType) throws IOException {
        String auth = token != null ? "Bearer " + token : authorization;
        HttpURLConnection connection = HttpConnections.open(uri, method, auth);
        if (accept != null) connection.setRequestProperty("Accept", accept.getMimeType());
        if (contentType != null) connection.setRequestProperty("Content-Type", contentType.getMimeType());
        return connection;
    }

    private static void write(HttpURLConnection connection, byte[] body) throws IOException {
        connection.setDoOutput(true);
        try (OutputStream out = connection.getOutputStream()) {
            out.write(body);
        }
    }

    private static byte[] read(HttpURLConnection connection, int status) throws IOException {
        InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream();
        if (in == null) return new byte[0];
        try (InputStream body = in) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int n;
            while ((n = body.read(buffer)) > 0) bytes.write(buffer, 0, n);
            return bytes.toByteArray();
        }
    }

    /** The id at the end of a resource URL. */
    private static String lastSegment(String location) {
        return location.substring(location.lastIndexOf('/') + 1);
    }

    @Override
    protected <T> CompletableFuture<T> sendGetRequest(URI uri, ResourceFormat format, Function<InputStream, T> bodyMapper) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                connection = open(uri, "GET", format, null);
                int status = connection.getResponseCode();
                byte[] body = read(connection, status);
                if (status == 200) return bodyMapper.apply(new ByteArrayInputStream(body));
                throw new CompletionException("HTTP error " + status + ": " + new String(body), null);
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) connection.disconnect();
            }
        });
    }

    @Override
    protected CompletableFuture<String> sendPostRequest(URI uri, ResourceFormat format, byte[] body) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                connection = open(uri, "POST", ResourceFormat.JSON, format);
                write(connection, body);
                int status = connection.getResponseCode();
                if (status == 201 || status == 303) {
                    String location = connection.getHeaderField("Location");
                    if (location == null) throw new IllegalStateException("Missing Location header in response.");
                    return lastSegment(location);
                }
                throw new CompletionException(connection.getResponseMessage(), null);
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) connection.disconnect();
            }
        });
    }

    @Override
    protected <T> CompletableFuture<T> sendPostRequestAndReadResponse(URI uri, ResourceFormat format, byte[] body,
                                                                    Function<InputStream, T> bodyMapper) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                connection = open(uri, "POST", ResourceFormat.JSON, format);
                write(connection, body);
                int status = connection.getResponseCode();
                byte[] response = read(connection, status);
                if (status == 200) return bodyMapper.apply(new ByteArrayInputStream(response));
                String text = new String(response);
                try {
                    // The node's own explanation, when it sent one.
                    throw new CompletionException(((JsonObject) JsonParser.parseString(text)).get("message").getAsString(), null);
                } catch (RuntimeException notJson) {
                    if (notJson instanceof CompletionException) throw notJson;
                    throw new CompletionException("HTTP error " + status + ": " + text, null);
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) connection.disconnect();
            }
        });
    }

    @Override
    protected CompletableFuture<Integer> sendPutRequest(URI uri, ResourceFormat format, byte[] body) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                connection = open(uri, "PUT", ResourceFormat.JSON, format);
                write(connection, body);
                return connection.getResponseCode();
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) connection.disconnect();
            }
        });
    }

    @Override
    protected CompletableFuture<Set<String>> sendBatchPostRequest(URI uri, ResourceFormat format, byte[] body) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                connection = open(uri, "POST", null, format);
                write(connection, body);
                int status = connection.getResponseCode();
                if (status != 201 && status != 303) {
                    throw new CompletionException(new ResourceParseException(connection.getResponseMessage()));
                }
                Set<String> ids = new LinkedHashSet<>();
                try (JsonReader reader = new JsonReader(new InputStreamReader(connection.getInputStream()))) {
                    reader.beginArray();
                    while (reader.hasNext()) ids.add(lastSegment(reader.nextString()));
                    reader.endArray();
                }
                return ids;
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) connection.disconnect();
            }
        });
    }
}
