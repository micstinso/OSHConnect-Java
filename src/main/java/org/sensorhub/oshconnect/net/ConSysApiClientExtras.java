package org.sensorhub.oshconnect.net;

import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.consys.obs.DataStreamBindingJson;
import org.sensorhub.impl.service.consys.obs.ObsHandler;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.impl.service.consys.system.SystemBindingGeoJson;
import org.sensorhub.impl.service.consys.task.CommandHandler;
import org.sensorhub.impl.service.consys.task.CommandStreamBindingJson;
import org.sensorhub.oshconnect.datamodels.CommandBindingJson;
import org.sensorhub.oshconnect.datamodels.CommandData;
import org.sensorhub.oshconnect.datamodels.ObservationBindingOmJson;
import org.sensorhub.oshconnect.datamodels.ObservationData;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ConSysApiClientExtras {
    static final String JSON_ARRAY_ITEMS = "items";
    static final String SYSTEMS_COLLECTION = "systems";
    static final String DATASTREAMS_COLLECTION = "datastreams";
    static final String CONTROLS_COLLECTION = "controlstreams";
    static final String OBSERVATIONS_COLLECTION = "observations";
    static final String COMMANDS_COLLECTION = "commands";
    static final String BINDING_ERROR = "Error initializing binding";

    protected URI endpoint;
    protected Authenticator authenticator;

    protected ConSysApiClientExtras() {
    }

    public static ConSysApiClientExtras.ConSysApiClientExtrasBuilder newBuilder(String endpoint) {
        Asserts.checkNotNull(endpoint, "endpoint");
        return new ConSysApiClientExtras.ConSysApiClientExtrasBuilder(endpoint);
    }

    /**
     * Get the systems of the OpenSensorHub node.
     *
     * @return A list of systems.
     */
    public CompletableFuture<List<ISystemWithDesc>> getSystems() {
        return getSystems(null);
    }

    /**
     * Get the systems of the OpenSensorHub node with a query string.
     *
     * @param queryString The query string to include in the request.
     * @return A list of systems matching the query string.
     */
    public CompletableFuture<List<ISystemWithDesc>> getSystems(String queryString) {
        if (queryString == null)
            queryString = "";

        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + queryString), ResourceFormat.JSON, body -> {
            try {
                var ctx = new RequestContext(body);

                JsonObject bodyJson = JsonParser.parseReader(new InputStreamReader(ctx.getInputStream())).getAsJsonObject();
                JsonArray features = bodyJson.getAsJsonArray(JSON_ARRAY_ITEMS);

                List<ISystemWithDesc> systems = new ArrayList<>();
                for (var feature : features) {
                    var ctx2 = new RequestContext(new ByteArrayInputStream(feature.toString().getBytes()));
                    var binding = new SystemBindingGeoJson(ctx2, null, null, true);
                    systems.add(binding.deserialize());
                }

                return systems;
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Get the data stream IDs for a system.
     *
     * @param systemID The ID of the system.
     * @return A list of data stream IDs.
     */
    public CompletableFuture<List<String>> getDataStreamIds(String systemID) {
        return getDataStreamIds(systemID, null);
    }

    /**
     * Get the data stream IDs for a system with a query string.
     *
     * @param systemID    The ID of the system.
     * @param queryString The query string to include in the request.
     * @return A list of data stream IDs matching the query string.
     */
    public CompletableFuture<List<String>> getDataStreamIds(String systemID, String queryString) {
        if (queryString == null)
            queryString = "";

        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemID + "/" + DATASTREAMS_COLLECTION + queryString), ResourceFormat.JSON, body -> {
            try {
                var ctx = new RequestContext(body);

                JsonObject bodyJson = JsonParser.parseReader(new InputStreamReader(ctx.getInputStream())).getAsJsonObject();
                JsonArray features = bodyJson.getAsJsonArray(JSON_ARRAY_ITEMS);

                List<String> dataStreams = new ArrayList<>();
                for (var feature : features) {
                    // Get the ID
                    var id = feature.getAsJsonObject().entrySet().stream()
                            .filter(e -> e.getKey().equals("id"))
                            .findFirst()
                            .orElseThrow(() -> new IOException("No id found in feature"));
                    dataStreams.add(id.getValue().getAsString());
                }

                return dataStreams;
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    public CompletableFuture<Map<String, Instant>> getLastObservationTimes(String systemID, String queryString) {
        if (queryString == null)
            queryString = "";

        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemID + "/" + DATASTREAMS_COLLECTION + queryString), ResourceFormat.JSON, body -> {
            try {
                var ctx = new RequestContext(body);
                JsonObject bodyJson = JsonParser.parseReader(new InputStreamReader(ctx.getInputStream())).getAsJsonObject();
                JsonArray features = bodyJson.getAsJsonArray(JSON_ARRAY_ITEMS);

                Map<String, Instant> times = new LinkedHashMap<>();
                for (var feature : features) {
                    var obj = feature.getAsJsonObject();
                    if (!obj.has("id"))
                        throw new IOException("No id found in feature");
                    times.put(obj.get("id").getAsString(), parseExtentEnd(obj.getAsJsonArray("phenomenonTime")));
                }
                return times;
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    private static Instant parseExtentEnd(JsonArray extent) {
        if (extent == null || extent.size() < 2)
            return null;
        var end = extent.get(1).getAsString();
        if ("0".equals(end))          // no observations yet
            return null;
        if ("now".equals(end))        // defensive: your node uses this on validTime, not phenomenonTime
            return Instant.now();
        try {
            return Instant.parse(end);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * Update a data stream.
     *
     * @param dataStreamId The ID of the data stream.
     * @param dataStream   The data stream object.
     * @return The status code of the operation.
     */
    public CompletableFuture<Integer> updateDataStream(String dataStreamId, IDataStreamInfo dataStream) {
        try {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new DataStreamBindingJson(ctx, null, null, false, Collections.emptyMap());
            binding.serialize(null, dataStream, false);

            return sendPutRequest(
                    endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStreamId),
                    ResourceFormat.JSON,
                    buffer.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    public CompletableFuture<Integer> deleteDataStream(String dataStreamId) {
        return sendDeleteRequest(endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStreamId));
    }

    /**
     * Push an observation to a data stream.
     *
     * @param dataStreamId The ID of the data stream.
     * @param dataStream   The data stream object.
     * @param obs          The observation to push.
     * @return The ID of the observation if the operation was successful, otherwise null.
     */
    public CompletableFuture<String> pushObservation(String dataStreamId, IDataStreamInfo dataStream, ObservationData obs) {
        try {
            ObsHandler.ObsHandlerContextData contextData = new ObsHandler.ObsHandlerContextData();
            contextData.dsInfo = dataStream;

            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            ctx.setData(contextData);

            ctx.setFormat(ResourceFormat.OM_JSON);
            var binding = new ObservationBindingOmJson(ctx, null, false);
            binding.serialize(null, obs, false);

            return sendPostRequest(
                    endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStreamId + "/" + OBSERVATIONS_COLLECTION),
                    ctx.getFormat(),
                    buffer.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * Get an observation by ID.
     *
     * @param observationId The ID of the observation.
     * @param dataStream    The data stream object.
     * @return The observation.
     */
    public CompletableFuture<ObservationData> getObservation(String observationId, IDataStreamInfo dataStream) {
        return sendGetRequest(endpoint.resolve(OBSERVATIONS_COLLECTION + "/" + observationId), ResourceFormat.OM_JSON, body -> {
            try {
                ObsHandler.ObsHandlerContextData contextData = new ObsHandler.ObsHandlerContextData();
                contextData.dsInfo = dataStream;

                var ctx = new RequestContext(body);
                ctx.setData(contextData);
                ctx.setFormat(ResourceFormat.OM_JSON);

                var binding = new ObservationBindingOmJson(ctx, null, true);
                return binding.deserialize();

            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Get the latest set of observations for a data stream.
     *
     * @param dataStreamId The ID of the data stream.
     * @param dataStream   The data stream object.
     * @return A list of observations.
     */
    public CompletableFuture<List<ObservationData>> getObservations(String dataStreamId, IDataStreamInfo dataStream) {
        return getObservations(dataStreamId, dataStream, "");
    }

    /**
     * Get a set of observations for a data stream with a query string.
     *
     * @param dataStreamId The ID of the data stream.
     * @param dataStream   The data stream object.
     * @param queryString  The query string to include in the request.
     *                     Valid parameters are:
     *                     <ul>
     *                     <li>id:
     *                     List of resource local IDs or unique IDs (URI).
     *                     Only resources that have one of the provided identifiers are selected.</li>
     *                     <li>phenomenonTime:
     *                     Only resources with a phenomenonTime property
     *                     that intersects the value of the phenomenonTime parameter are selected.</li>
     *                     <li>resultTime:
     *                     Only resources with a phenomenonTime property
     *                     that intersects the value of the phenomenonTime parameter are selected.</li>
     *                     <li>foi:
     *                     List of feature local IDs or unique IDs (URI).
     *                     Only resources that are associated with a feature of interest
     *                     that has one of the provided identifiers are selected.</li>
     *                     <li>observedProperty:
     *                     List of property local IDs or unique IDs (URI).
     *                     Only resources that are associated with an observable property
     *                     that has one of the provided identifiers are selected.</li>
     *                     <li>limit:
     *                     Limits the number of items that are presented in the response.</li>
     *                     </ul>
     * @return A list of observations.
     */
    public CompletableFuture<List<ObservationData>> getObservations(String dataStreamId, IDataStreamInfo dataStream, String queryString) {
        if (queryString == null)
            queryString = "";
        if (!queryString.isEmpty() && !queryString.startsWith("?"))
            queryString = "?" + queryString;

        String url = DATASTREAMS_COLLECTION + "/" + dataStreamId + "/" + OBSERVATIONS_COLLECTION + queryString;

        return sendGetRequest(endpoint.resolve(url), ResourceFormat.OM_JSON, body -> {
            try {
                ObsHandler.ObsHandlerContextData contextData = new ObsHandler.ObsHandlerContextData();
                contextData.dsInfo = dataStream;

                var ctx = new RequestContext(body);

                JsonObject bodyJson = JsonParser.parseReader(new InputStreamReader(ctx.getInputStream())).getAsJsonObject();
                JsonArray features = bodyJson.getAsJsonArray(JSON_ARRAY_ITEMS);

                List<ObservationData> observations = new ArrayList<>();
                for (var feature : features) {
                    var ctx2 = new RequestContext(new ByteArrayInputStream(feature.toString().getBytes()));
                    ctx2.setData(contextData);
                    ctx2.setFormat(ResourceFormat.OM_JSON);

                    var binding = new ObservationBindingOmJson(ctx2, null, true);
                    observations.add(binding.deserialize());
                }

                return observations;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Get the control stream IDs for a system.
     *
     * @param systemID The ID of the system.
     * @return A list of control stream IDs.
     */
    public CompletableFuture<List<String>> getControlStreamIds(String systemID) {
        return getControlStreamIds(systemID, null);
    }

    /**
     * Get the control stream IDs for a system with a query string.
     *
     * @param systemID    The ID of the system.
     * @param queryString The query string to include in the request.
     * @return A list of control stream IDs matching the query string.
     */
    public CompletableFuture<List<String>> getControlStreamIds(String systemID, String queryString) {
        if (queryString == null)
            queryString = "";

        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemID + "/" + CONTROLS_COLLECTION + queryString), ResourceFormat.JSON, body -> {
            try {
                var ctx = new RequestContext(body);

                JsonObject bodyJson = JsonParser.parseReader(new InputStreamReader(ctx.getInputStream())).getAsJsonObject();
                JsonArray features = bodyJson.getAsJsonArray(JSON_ARRAY_ITEMS);

                List<String> controlStreams = new ArrayList<>();
                for (var feature : features) {
                    // Get the ID
                    var id = feature.getAsJsonObject().entrySet().stream()
                            .filter(e -> e.getKey().equals("id"))
                            .findFirst()
                            .orElseThrow(() -> new IOException("No id found in feature"));
                    controlStreams.add(id.getValue().getAsString());
                }

                return controlStreams;
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Update a control stream.
     *
     * @param controlStreamId The ID of the control stream.
     * @param controlStream   The control stream object.
     * @return The status code of the operation.
     */
    public CompletableFuture<Integer> updateControlStream(String controlStreamId, ICommandStreamInfo controlStream) {
        try {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new CommandStreamBindingJson(ctx, null, null, false);
            binding.serialize(null, controlStream, false);

            return sendPutRequest(
                    endpoint.resolve(CONTROLS_COLLECTION + "/" + controlStreamId),
                    ResourceFormat.JSON,
                    buffer.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * Delete a control stream.
     *
     * @param controlStreamId The ID of the control stream.
     * @return The status code of the operation.
     */
    public CompletableFuture<Integer> deleteControlStream(String controlStreamId) {
        return sendDeleteRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + controlStreamId));
    }

    /**
     * Push a command to a control stream.
     *
     * @param controlStreamId   The ID of the control stream.
     * @param commandStreamInfo The control stream object.
     * @param command           The command to push.
     * @return The ID of the command if the operation was successful, otherwise null.
     */
    public CompletableFuture<String> pushCommand(String controlStreamId, ICommandStreamInfo commandStreamInfo, CommandData command) {
        try {
            CommandHandler.CommandHandlerContextData contextData = new CommandHandler.CommandHandlerContextData();
            contextData.csInfo = commandStreamInfo;

            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            ctx.setData(contextData);
            ctx.setFormat(ResourceFormat.JSON);

            var binding = new CommandBindingJson(ctx, null, false);
            binding.serialize(null, command, false);

            return sendPostRequest(
                    endpoint.resolve(CONTROLS_COLLECTION + "/" + controlStreamId + "/" + COMMANDS_COLLECTION),
                    ctx.getFormat(),
                    buffer.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * Get a command by ID.
     *
     * @param controlStreamId   The ID of the control stream.
     * @param commandId         The ID of the command.
     * @param commandStreamInfo The control stream object.
     * @return The command.
     */
    public CompletableFuture<CommandData> getCommand(String controlStreamId, String commandId, ICommandStreamInfo commandStreamInfo) {
        return sendGetRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + controlStreamId + "/" + COMMANDS_COLLECTION + "/" + commandId), ResourceFormat.OM_JSON, body -> {
            try {
                CommandHandler.CommandHandlerContextData contextData = new CommandHandler.CommandHandlerContextData();
                contextData.csInfo = commandStreamInfo;

                var ctx = new RequestContext(body);
                ctx.setData(contextData);
                ctx.setFormat(ResourceFormat.OM_JSON);

                var binding = new CommandBindingJson(ctx, null, true);
                return binding.deserialize();

            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Get the latest set of commands for a control stream.
     *
     * @param controlStreamId   The ID of the control stream.
     * @param commandStreamInfo The control stream object.
     * @return A list of commands.
     */
    public CompletableFuture<List<CommandData>> getCommands(String controlStreamId, ICommandStreamInfo commandStreamInfo) {
        return getCommands(controlStreamId, commandStreamInfo, "");
    }

    /**
     * Get a set of commands for a control stream with a query string.
     *
     * @param controlStreamId   The ID of the control stream.
     * @param commandStreamInfo The control stream object.
     * @param queryString       The query string to include in the request.
     * @return A list of commands.
     */
    public CompletableFuture<List<CommandData>> getCommands(String controlStreamId, ICommandStreamInfo commandStreamInfo, String queryString) {
        if (queryString == null)
            queryString = "";

        String url = CONTROLS_COLLECTION + "/" + controlStreamId + "/" + COMMANDS_COLLECTION + queryString;

        return sendGetRequest(endpoint.resolve(url), ResourceFormat.JSON, body -> {
            try {
                CommandHandler.CommandHandlerContextData contextData = new CommandHandler.CommandHandlerContextData();
                contextData.csInfo = commandStreamInfo;

                var ctx = new RequestContext(body);
                JsonObject bodyJson = JsonParser.parseReader(new InputStreamReader(ctx.getInputStream())).getAsJsonObject();
                JsonArray features = bodyJson.getAsJsonArray(JSON_ARRAY_ITEMS);

                List<CommandData> observations = new ArrayList<>();
                for (var feature : features) {
                    var ctx2 = new RequestContext(new ByteArrayInputStream(feature.toString().getBytes()));
                    ctx2.setData(contextData);
                    ctx2.setFormat(ResourceFormat.OM_JSON);

                    var binding = new CommandBindingJson(ctx2, null, true);
                    observations.add(binding.deserialize());
                }

                return observations;
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    protected <T> CompletableFuture<T> sendGetRequest(URI collectionUri, ResourceFormat format, Function<InputStream, T> bodyMapper) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                if (authenticator != null) {
                    Authenticator.setDefault(authenticator);
                }
                connection.setRequestMethod("GET");
                connection.setRequestProperty(HttpHeaders.ACCEPT, format.getMimeType());

                int responseCode = connection.getResponseCode();
                if (responseCode == 200) {
                    try (InputStream is = connection.getInputStream()) {
                        return bodyMapper.apply(is);
                    }
                } else {
                    throw new CompletionException("HTTP error " + responseCode, null);
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }

    protected CompletableFuture<String> sendPostRequest(URI collectionUri, ResourceFormat format, byte[] body) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                if (authenticator != null) {
                    Authenticator.setDefault(authenticator);
                }
                connection.setRequestMethod("POST");
                connection.setRequestProperty(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType());
                connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, format.getMimeType());
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(body);
                }

                int responseCode = connection.getResponseCode();
                if (responseCode == 201 || responseCode == 303) {
                    String location = connection.getHeaderField(HttpHeaders.LOCATION);
                    if (location == null) {
                        throw new IllegalStateException("Missing Location header in response");
                    }
                    return location.substring(location.lastIndexOf('/') + 1);
                } else if (responseCode == 200) {
                    try (InputStream is = connection.getInputStream()) {
                        return new String(is.readAllBytes());
                    }
                } else {
                    throw new CompletionException("HTTP error " + responseCode, null);
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }

    protected CompletableFuture<Integer> sendPutRequest(URI collectionUri, ResourceFormat format, byte[] body) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                if (authenticator != null) {
                    Authenticator.setDefault(authenticator);
                }
                connection.setRequestMethod("PUT");
                connection.setRequestProperty(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType());
                connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, format.getMimeType());
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(body);
                }

                return connection.getResponseCode();
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }

    protected CompletableFuture<Integer> sendDeleteRequest(URI collectionUri) {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                if (authenticator != null) {
                    Authenticator.setDefault(authenticator);
                }
                connection.setRequestMethod("DELETE");
                connection.setRequestProperty(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType());

                return connection.getResponseCode();
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }

    public static class ConSysApiClientExtrasBuilder extends BaseBuilder<ConSysApiClientExtras> {
        ConSysApiClientExtrasBuilder(String endpoint) {
            this.instance = new ConSysApiClientExtras();

            try {
                if (!endpoint.endsWith("/"))
                    endpoint += "/";
                instance.endpoint = new URI(endpoint);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URI " + endpoint);
            }
        }

        public ConSysApiClientExtras.ConSysApiClientExtrasBuilder simpleAuth(String user, char[] password) {
            if (!Strings.isNullOrEmpty(user)) {
                var finalPwd = password != null ? password : new char[0];
                instance.authenticator = new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(user, finalPwd);
                    }
                };
            }

            return this;
        }

        @Override
        public ConSysApiClientExtras build() {
            return instance;
        }
    }
}
