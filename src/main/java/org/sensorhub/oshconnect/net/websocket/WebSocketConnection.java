/**
 * Copyright (c) 2023. Botts Innovative Research, Inc.
 * All Rights Reserved.
 */
package org.sensorhub.oshconnect.net.websocket;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.sensorhub.oshconnect.OSHNode;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;


/**
 * Class representing an active connection to a WebSocket.
 */
public class WebSocketConnection implements WebSocketListener {
    private final StreamListener streamListener;
    private final String request;
    private WebSocketClient client = null; //new WebSocketClient();
    private final List<StatusListener> statusListeners = new ArrayList<>();
    private StreamStatus status = StreamStatus.DISCONNECTED;
    private SSLContext sslContext = null;
    private final SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();

    public WebSocketConnection(StreamListener streamListener, String request) {
        this.streamListener = streamListener;
        this.request = request;
    }

    public void connect() {
        if (status != StreamStatus.CONNECTED) {
            OSHNode parentNode = streamListener.getDataStream().getParentSystem().getParentNode();
            String urlString = parentNode.getWSPrefix() + request;
            ClientUpgradeRequest clientUpgradeRequest = new ClientUpgradeRequest();
            clientUpgradeRequest.setHeader("Authorization", "Basic " + parentNode.getAuthorizationToken());

            try {

                TrustManager[] trustAllCerts = new TrustManager[]{
                        new X509TrustManager() {
                            @Override
                            public X509Certificate[] getAcceptedIssuers() {
                                return new X509Certificate[0];
                            }

                            @Override
                            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

                            @Override
                            public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                        }
                };

                if ( null == sslContext ) {
                    sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(null, trustAllCerts, new SecureRandom());
                    SSLContext.setDefault(sslContext);

                    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
                    HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

                    sslContextFactory.setSslContext(sslContext);

                }
                sslContextFactory.getSslContext().getClientSessionContext().setSessionTimeout(0);
                sslContextFactory.getSslContext().getClientSessionContext().setSessionCacheSize(0);

                client = new WebSocketClient(sslContextFactory);
                client.getPolicy().setMaxBinaryMessageSize(4 * 1024 * 1024);   // Jetty 9.x
//                client.setMaxBinaryMessageSize(4 * 1024 * 1024);               // Jetty 10/11+

                client.start();
                client.connect(this, new URI(urlString), clientUpgradeRequest);
            } catch (Exception e) {
                updateStatus(StreamStatus.ERROR);
            }
        }
    }

    public void disconnect() {
        try {
            client.stop();
        } catch (Exception e) {
            updateStatus(StreamStatus.ERROR);
        }
    }

    @Override
    public void onWebSocketBinary(byte[] bytes, int i, int i1) {
        streamListener.onStreamUpdate(bytes);
    }

    @Override
    public void onWebSocketText(String s) {
        // Not implemented
    }

    @Override
    public void onWebSocketClose(int i, String s) {
        updateStatus(StreamStatus.DISCONNECTED);
    }

    @Override
    public void onWebSocketConnect(Session session) {
        updateStatus(StreamStatus.CONNECTED);
    }

    @Override
    public void onWebSocketError(Throwable throwable) {
        updateStatus(StreamStatus.ERROR);
    }

    public void addStatusListener(StatusListener listener) {
        statusListeners.add(listener);
    }

    public void removeStatusListener(StatusListener listener) {
        statusListeners.remove(listener);
    }

    private void updateStatus(StreamStatus newStatus) {
        status = newStatus;
        for (StatusListener listener : statusListeners) {
            listener.onStatusChanged(newStatus);
        }
    }

    public StreamStatus getStatus() {
        return status;
    }
}
