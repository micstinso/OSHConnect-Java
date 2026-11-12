package org.sensorhub.oshconnect.net.tls;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.net.HttpURLConnection;
import java.util.function.Supplier;

/**
 * The TLS trust for OSHConnect's connections (REST, osh-core's ConSysApiClient,
 * WebSockets). By default the platform's own: its trusted CAs, with the usual
 * hostname check. An application that needs something else, e.g. to trust a
 * node's self-signed certificate after asking the user, calls
 * {@link #configure(Supplier, HostnameVerifier)}.
 *
 * <p>HTTP connections share one context (and so can resume each other's TLS
 * sessions); each WebSocket gets a context of its own. Android's TLS library
 * (BoringSSL) refuses to resume, in Jetty's SSLEngine, a session made by an
 * HttpsURLConnection socket ("ATTEMPT_TO_REUSE_SESSION_IN_DIFFERENT_CONTEXT"),
 * so the two must not share a session cache.
 *
 * <p>Applied to each connection; nothing process-wide is ever changed, so a
 * host application (and anything else in its process) keeps its own settings.
 */
public final class TlsSettings {
    private static Supplier<SSLContext> contextFactory;
    private static HostnameVerifier hostnameVerifier;
    private static SSLContext httpContext;

    private TlsSettings() {
    }

    /**
     * @param contextFactory   makes the contexts for connections from now on; null
     *                         for the platform's
     * @param hostnameVerifier for {@code HttpsURLConnection}, which on Android checks
     *                         hostnames in a verifier rather than in the trust
     *                         manager; null for the platform's (on the JVM, leave it
     *                         null: the default verifier has the trust manager check)
     */
    public static synchronized void configure(Supplier<SSLContext> contextFactory, HostnameVerifier hostnameVerifier) {
        TlsSettings.contextFactory = contextFactory;
        TlsSettings.hostnameVerifier = hostnameVerifier;
        httpContext = null;
    }

    /** The context HTTP connections share, or null for the platform's. */
    private static synchronized SSLContext httpContext() {
        if (httpContext == null && contextFactory != null) httpContext = contextFactory.get();
        return httpContext;
    }

    /** A new context for one WebSocket connection, or null for the platform's. */
    public static SSLContext newWebSocketContext() {
        Supplier<SSLContext> factory;
        synchronized (TlsSettings.class) {
            factory = contextFactory;
        }
        return factory != null ? factory.get() : null;
    }

    /** Use these settings for {@code connection} if it is HTTPS. Call before connecting. */
    public static void apply(HttpURLConnection connection) {
        if (!(connection instanceof HttpsURLConnection)) return;
        HttpsURLConnection https = (HttpsURLConnection) connection;
        SSLContext context = httpContext();
        if (context != null) https.setSSLSocketFactory(context.getSocketFactory());
        HostnameVerifier verifier;
        synchronized (TlsSettings.class) {
            verifier = hostnameVerifier;
        }
        if (verifier != null) https.setHostnameVerifier(verifier);
    }
}
