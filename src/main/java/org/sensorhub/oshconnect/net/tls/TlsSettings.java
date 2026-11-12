package org.sensorhub.oshconnect.net.tls;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.net.HttpURLConnection;

/**
 * The TLS trust for OSHConnect's connections (REST, osh-core's ConSysApiClient,
 * WebSockets). By default the platform's own: its trusted CAs, with the usual
 * hostname check. An application that needs something else, e.g. to trust a
 * node's self-signed certificate after asking the user, calls
 * {@link #configure(SSLContext, HostnameVerifier)}.
 *
 * <p>Applied to each connection; nothing process-wide is ever changed, so a
 * host application (and anything else in its process) keeps its own settings.
 */
public final class TlsSettings {
    private static volatile SSLContext sslContext;
    private static volatile HostnameVerifier hostnameVerifier;

    private TlsSettings() {
    }

    /**
     * @param sslContext       for every connection from now on; null for the platform's
     * @param hostnameVerifier for {@code HttpsURLConnection}, which on Android checks
     *                         hostnames in a verifier rather than in the trust
     *                         manager; null for the platform's (on the JVM, leave it
     *                         null: the default verifier has the trust manager check)
     */
    public static void configure(SSLContext sslContext, HostnameVerifier hostnameVerifier) {
        TlsSettings.sslContext = sslContext;
        TlsSettings.hostnameVerifier = hostnameVerifier;
    }

    /** The configured context, or null for the platform's. */
    public static SSLContext sslContext() {
        return sslContext;
    }

    /** Use these settings for {@code connection} if it is HTTPS. Call before connecting. */
    public static void apply(HttpURLConnection connection) {
        if (!(connection instanceof HttpsURLConnection)) return;
        HttpsURLConnection https = (HttpsURLConnection) connection;
        SSLContext context = sslContext;
        if (context != null) https.setSSLSocketFactory(context.getSocketFactory());
        HostnameVerifier verifier = hostnameVerifier;
        if (verifier != null) https.setHostnameVerifier(verifier);
    }
}
