package org.sensorhub.oshconnect.net;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.oshconnect.net.tls.TlsSettings;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OSHConnect's connections use the platform's trust until an application
 * configures its own (TlsSettings), and then exactly that, on every path:
 * HttpConnections / ConSysApiClientExtras, osh-core's client (OshConSysApiClient,
 * all five request methods) and Jetty (WebSockets, set up as WebSocketConnection
 * does). Credentials go on each connection, and nothing process-wide changes.
 */
class TlsSettingsTest {
    private static final String PASS = "changeit";
    private static final String BASIC = "Basic " + Base64.getEncoder().encodeToString("bob:secret".getBytes(StandardCharsets.UTF_8));

    @TempDir
    File dir;
    private HttpsServer server;
    private String root;
    private SSLContext trustsServer;
    private javax.net.ssl.TrustManager[] trustManagers;
    /** "METHOD path Authorization" of each request. */
    private final List<String> requests = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        File keystore = new File(dir, "self.p12");
        Process keytool = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/keytool").getPath(),
                "-genkeypair", "-alias", "s", "-keyalg", "EC", "-dname", "CN=127.0.0.1", "-ext", "san=ip:127.0.0.1",
                "-validity", "2", "-keystore", keystore.getPath(), "-storepass", PASS, "-storetype", "PKCS12")
                .redirectErrorStream(true).start();
        assertEquals(0, keytool.waitFor(), new String(keytool.getInputStream().readAllBytes()));

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = new FileInputStream(keystore)) {
            ks.load(in, PASS.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, PASS.toCharArray());
        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(serverContext));
        server.createContext("/", this::respond);
        server.start();
        root = "https://127.0.0.1:" + server.getAddress().getPort() + "/api/";

        // What an application might configure: trust in the server's certificate.
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        trust.setCertificateEntry("server", ks.getCertificate("s"));
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        trustManagers = tmf.getTrustManagers();
        trustsServer = SSLContext.getInstance("TLSv1.3");
        trustsServer.init(null, trustManagers, null);
    }

    private void respond(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        exchange.getRequestBody().readAllBytes();
        requests.add(exchange.getRequestMethod() + " " + path + " " + exchange.getRequestHeaders().getFirst("Authorization"));
        String body = "";
        int status = 200;
        switch (path) {
            case "/api/get":
                body = "hello";
                break;
            case "/api/post":
                exchange.getResponseHeaders().add("Location", root + "things/42");
                status = 201;
                break;
            case "/api/batch":
                body = "[\"" + root + "things/a\",\"" + root + "things/b\"]";
                status = 201;
                break;
            case "/api/put":
                status = 204;
                break;
            case "/api/command":
                body = "{\"ok\":true}";
                break;
            default:
                status = 404;
                body = "nothing here";
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @AfterEach
    void tearDown() {
        TlsSettings.configure(null, null);
        server.stop(0);
    }

    private boolean get() {
        try {
            HttpURLConnection connection = HttpConnections.open(URI.create(root + "get"), "GET", null);
            return connection.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean jetty() {
        SslContextFactory.Client factory = new SslContextFactory.Client();
        SSLContext context = TlsSettings.newWebSocketContext();
        if (context != null) factory.setSslContext(context);
        factory.setEndpointIdentificationAlgorithm("HTTPS");
        HttpClient client = new HttpClient(factory);
        try {
            client.start();
            return client.GET(root + "get").getStatus() == 200;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                client.stop();
            } catch (Exception ignored) {
                // closing
            }
        }
    }

    private static String text(InputStream in) {
        try {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    @Test
    void platformTrustByDefault() {
        assertFalse(get());
        assertFalse(jetty());
        OshConSysApiClient client = new OshConSysApiClient(root, null, null);
        assertThrows(Exception.class, () -> client.sendGetRequest(URI.create(root + "get"), ResourceFormat.JSON, TlsSettingsTest::text).join());
    }

    @Test
    void configuredTrustIsUsedForHttpAndWebSockets() {
        TlsSettings.configure(() -> trustsServer, null);
        assertTrue(get());
        assertTrue(jetty());
        TlsSettings.configure(null, null);
        assertFalse(get());
        assertFalse(jetty());
    }

    /**
     * HTTP connections share one context; each WebSocket gets its own, so Jetty
     * never tries to resume a session an HttpsURLConnection made (which Android's
     * BoringSSL rejects: ATTEMPT_TO_REUSE_SESSION_IN_DIFFERENT_CONTEXT).
     */
    @Test
    void webSocketsDoNotShareContextsWithHttp() {
        java.util.concurrent.atomic.AtomicInteger made = new java.util.concurrent.atomic.AtomicInteger();
        TlsSettings.configure(() -> {
            made.incrementAndGet();
            try {
                SSLContext context = SSLContext.getInstance("TLSv1.3");
                context.init(null, trustManagers, null);
                return context;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, null);

        assertTrue(get());
        assertTrue(get());
        assertEquals(1, made.get(), "HTTP connections share one context");

        SSLContext first = TlsSettings.newWebSocketContext();
        SSLContext second = TlsSettings.newWebSocketContext();
        assertFalse(first == second, "each WebSocket gets its own context");
        assertEquals(3, made.get());
        assertTrue(jetty());
    }

    @Test
    void oshCoreRequestsUseTheConfiguredTrustAndCredentials() {
        TlsSettings.configure(() -> trustsServer, null);
        OshConSysApiClient client = new OshConSysApiClient(root.substring(0, root.length() - 1), "bob", "secret".toCharArray());
        URI api = URI.create(root);

        assertEquals("hello", client.sendGetRequest(api.resolve("get"), ResourceFormat.JSON, TlsSettingsTest::text).join());
        assertEquals("42", client.sendPostRequest(api.resolve("post"), ResourceFormat.JSON, "{}".getBytes()).join());
        assertEquals(204, client.sendPutRequest(api.resolve("put"), ResourceFormat.JSON, "{}".getBytes()).join());
        assertEquals(Set.of("a", "b"), client.sendBatchPostRequest(api.resolve("batch"), ResourceFormat.JSON, "[]".getBytes()).join());
        assertEquals("{\"ok\":true}", client.sendPostRequestAndReadResponse(api.resolve("command"), ResourceFormat.JSON, "{}".getBytes(), TlsSettingsTest::text).join());

        CompletionException notFound = assertThrows(CompletionException.class,
                () -> client.sendGetRequest(api.resolve("missing"), ResourceFormat.JSON, TlsSettingsTest::text).join());
        assertTrue(notFound.getMessage().contains("HTTP error 404"), notFound.getMessage());

        assertEquals(6, requests.size());
        requests.forEach(request -> assertTrue(request.endsWith(" " + BASIC), request));
    }

    @Test
    void extrasSendCredentialsOnTheConnection() {
        TlsSettings.configure(() -> trustsServer, null);
        ConSysApiClientExtras extras = ConSysApiClientExtras.newBuilder(root).simpleAuth("bob", "secret".toCharArray()).build();
        assertEquals("hello", extras.sendGetRequest(URI.create(root + "get"), ResourceFormat.JSON, TlsSettingsTest::text).join());
        assertEquals("GET /api/get " + BASIC, requests.get(0));
    }

    @Test
    void nothingProcessWideChanges() {
        SSLSocketFactory factory = HttpsURLConnection.getDefaultSSLSocketFactory();
        SSLContext defaultContext;
        try {
            defaultContext = SSLContext.getDefault();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        TlsSettings.configure(() -> trustsServer, null);
        new OshConSysApiClient(root, "bob", "secret".toCharArray())
                .sendGetRequest(URI.create(root + "get"), ResourceFormat.JSON, TlsSettingsTest::text).join();
        ConSysApiClientExtras.newBuilder(root).simpleAuth("bob", "secret".toCharArray()).build()
                .sendGetRequest(URI.create(root + "get"), ResourceFormat.JSON, TlsSettingsTest::text).join();

        assertSame(factory, HttpsURLConnection.getDefaultSSLSocketFactory());
        try {
            assertSame(defaultContext, SSLContext.getDefault());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertNull(Authenticator.getDefault(), "credentials must not go to the process-wide Authenticator");
    }
}
