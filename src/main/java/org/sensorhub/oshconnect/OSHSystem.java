package org.sensorhub.oshconnect;

import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.consys.client.ConSysApiClient;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.oshconnect.constants.Service;
import org.sensorhub.oshconnect.net.ConSysApiClientExtras;
import org.sensorhub.oshconnect.notification.INotificationControlStream;
import org.sensorhub.oshconnect.notification.INotificationDataStream;
import org.sensorhub.oshconnect.util.ControlStreamsQueryBuilder;
import org.sensorhub.oshconnect.util.DataStreamsQueryBuilder;
import org.sensorhub.oshconnect.util.Utilities;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Class representing an OpenSensorHub system.
 */
public class OSHSystem {
    private final OSHNode parentNode;
    private final Set<OSHDataStream> dataStreams = new HashSet<>();
    private final Set<OSHControlStream> controlStreams = new HashSet<>();
    private final Set<INotificationDataStream> dataStreamNotificationListeners = new HashSet<>();
    private final Set<INotificationControlStream> controlStreamNotificationListeners = new HashSet<>();
    private ISystemWithDesc systemResource;

    public OSHSystem(OSHNode parentNode, ISystemWithDesc systemResource) {
        this.parentNode = parentNode;
        this.systemResource = systemResource;
    }

    /**
     * Query the node for the data streams associated with the system.
     * New data streams will be added to the list of discovered data streams,
     * and existing data streams will have their resources updated.
     *
     * @return The list of discovered data streams.
     */
    public List<OSHDataStream> discoverDataStreams() throws ExecutionException, InterruptedException {
        return discoverDataStreams("");
    }

    /**
     * Query the node for the data streams associated with the system with a specific query.
     * New data streams will be added to the list of discovered data streams,
     * and existing data streams will have their resources updated.
     *
     * @param query The query to filter the data streams.
     * @return The list of discovered data streams matching the query.
     */
    public List<OSHDataStream> discoverDataStreams(DataStreamsQueryBuilder query) throws ExecutionException, InterruptedException {
        return discoverDataStreams(query.getQueryString());
    }

    /**
     * Query the node for the data streams associated with the system with a specific query.
     * New data streams will be added to the list of discovered data streams,
     * and existing data streams will have their resources updated.
     *
     * @param query The query to filter the data streams.
     * @return The list of discovered data streams matching the query.
     */
    public List<OSHDataStream> discoverDataStreams(String query) throws ExecutionException, InterruptedException {
        var dataStreamIds = getConnectedSystemsApiClientExtras().getDataStreamIds(getId(), query).get();
        List<OSHDataStream> result = new ArrayList<>();

        for (var id : dataStreamIds) {
            IDataStreamInfo dataStreamResource = null;

            try {
                dataStreamResource = getConnectedSystemsApiClient().getDatastreamById(id, ResourceFormat.OM_JSON, true).get();
            } catch ( Exception e ) {

                //in some cases fetching the schema from a live node doesn't work because ConSysApiClient defaults to swe+json
                //for the request which may not be available
                if (e.getCause() instanceof java.util.concurrent.CompletionException || e.getCause() instanceof org.sensorhub.impl.service.consys.ResourceParseException) {
                    if (e.getCause().getMessage().contains("Unsupported format") ||
                        e.getCause().getMessage().contains("Invalid JSON" ) ||
                        e.getCause().getMessage().contains("HTTP error 400") ) {
                        dataStreamResource = getConnectedSystemsApiClient().getDatastreamById(id, ResourceFormat.OM_JSON, false).get();
                    } else {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new RuntimeException(e);
                }
            }

            var dataStream = addOrUpdateDataStream(id, dataStreamResource);
            if (dataStream != null) {
                result.add(dataStream);
            }
        }
        return result;
    }

    public boolean isLive(Duration window) throws ExecutionException, InterruptedException {
        var cutoff = Instant.now().minus(window);
        return getConnectedSystemsApiClientExtras()
                .getLastObservationTimes(getId(), "").get()
                .values().stream()
                .anyMatch(t -> t != null && t.isAfter(cutoff));
    }

    /**
     * Add or update a data stream in the list of data streams and notify listeners.
     *
     * @param id                 The ID of the data stream.
     * @param dataStreamResource The data stream properties.
     * @return The added or updated data stream.
     */
    private OSHDataStream addOrUpdateDataStream(String id, IDataStreamInfo dataStreamResource) throws ExecutionException, InterruptedException {
        if (dataStreamResource == null) return null;

        var existingDataStream = dataStreams.stream()
                .filter(ds -> ds.getId().equals(id))
                .findFirst()
                .orElse(null);

        if (existingDataStream == null) {
            OSHDataStream newDataStream = new OSHDataStream(this, id, dataStreamResource);
            dataStreams.add(newDataStream);
            notifyDataStreamAdded(newDataStream);
            return newDataStream;
        } else {
            existingDataStream.setDataStreamResource(dataStreamResource);
            return existingDataStream;
        }
    }

    /**
     * Query the node for the control streams associated with the system.
     *
     * @return The list of discovered control streams.
     */
    public List<OSHControlStream> discoverControlStreams() throws ExecutionException, InterruptedException {
        return discoverControlStreams("");
    }

    /**
     * Query the node for the control streams associated with the system with a specific query.
     *
     * @param query The query to filter the control streams.
     * @return The list of discovered control streams matching the query.
     */
    public List<OSHControlStream> discoverControlStreams(ControlStreamsQueryBuilder query) throws ExecutionException, InterruptedException {
        return discoverControlStreams(query.getQueryString());
    }

    /**
     * Query the node for the control streams associated with the system with a specific query.
     *
     * @return The list of discovered control streams matching the query.
     */
    public List<OSHControlStream> discoverControlStreams(String query) throws ExecutionException, InterruptedException {
        var controlStreamIds = getConnectedSystemsApiClientExtras().getControlStreamIds(getId(), query).get();
        List<OSHControlStream> result = new ArrayList<>();
        for (var id : controlStreamIds) {
            var controlStreamResource = getConnectedSystemsApiClient().getControlStreamById(id, ResourceFormat.JSON, true).get();
            var controlStream = addOrUpdateControlStream(id, controlStreamResource);
            if (controlStream != null) {
                result.add(controlStream);
            }
        }

        return result;
    }

    /**
     * Add or update a control stream in the list of control streams and notify listeners.
     *
     * @param id                    The ID of the control stream.
     * @param controlStreamResource The control stream properties.
     * @return The added or updated control stream.
     */
    private OSHControlStream addOrUpdateControlStream(String id, ICommandStreamInfo controlStreamResource) {
        if (controlStreamResource == null) return null;

        var existingControlStream = controlStreams.stream()
                .filter(cs -> cs.getId().equals(id))
                .findFirst()
                .orElse(null);

        if (existingControlStream == null) {
            OSHControlStream newControlStream = new OSHControlStream(this, id, controlStreamResource);
            controlStreams.add(newControlStream);
            notifyControlStreamAdded(newControlStream);
            return newControlStream;
        } else {
            existingControlStream.setControlStreamResource(controlStreamResource);
            return existingControlStream;
        }
    }

    /**
     * Update the system properties on the server.
     * Note: After updating the system, the system properties are refreshed from the server,
     * not set to the provided system properties.
     *
     * @param systemResource The new system properties.
     * @return True if the update was successful, false otherwise.
     */
    public boolean updateSystem(ISystemWithDesc systemResource) throws ExecutionException, InterruptedException {
        Integer response = getConnectedSystemsApiClient().updateSystem(getId(), systemResource).get();
        boolean success = response != null && response >= 200 && response < 300;

        if (success) {
            return refreshSystem();
        }

        return false;
    }

    /**
     * Refresh the system properties from the server.
     *
     * @return True if the refresh was successful, false otherwise.
     */
    public boolean refreshSystem() throws ExecutionException, InterruptedException {
        var newSystemResource = getConnectedSystemsApiClient().getSystemById(getId(), ResourceFormat.JSON).get();

        if (newSystemResource != null) {
            systemResource = newSystemResource;
        }

        return newSystemResource != null;
    }

    /**
     * Create a new data stream associated with the system.
     *
     * @param dataStreamResource The data stream properties.
     * @return The new data stream or null if the creation failed.
     */
    public OSHDataStream createDataStream(DataStreamInfo dataStreamResource) throws ExecutionException, InterruptedException {
        String id = getConnectedSystemsApiClient().addDataStream(getId(), dataStreamResource).get();
        if (id == null) return null;

        var newDataStreamResource = getConnectedSystemsApiClient().getDatastreamById(id, ResourceFormat.OM_JSON, true).get();
        if (newDataStreamResource == null) return null;

        OSHDataStream dataStream = new OSHDataStream(this, id, newDataStreamResource);
        dataStreams.add(dataStream);
        notifyDataStreamAdded(dataStream);
        return dataStream;
    }

    /**
     * Delete a data stream associated with the system.
     *
     * @param dataStream The data stream to delete.
     * @return True if the deletion was successful, false otherwise.
     */
    public boolean deleteDataStream(OSHDataStream dataStream) throws ExecutionException, InterruptedException {
        Integer response = getConnectedSystemsApiClientExtras().deleteDataStream(dataStream.getId()).get();
        boolean success = response != null && response >= 200 && response < 300;

        if (success) {
            dataStreams.remove(dataStream);
            notifyDataStreamRemoved(dataStream);
        }

        return success;
    }

    /**
     * Create a new control stream associated with the system.
     *
     * @param controlStreamResource The control stream properties.
     * @return The new control stream or null if the creation failed.
     */
    public OSHControlStream createControlStream(CommandStreamInfo controlStreamResource) throws ExecutionException, InterruptedException {
        String id = getConnectedSystemsApiClient().addControlStream(getId(), controlStreamResource).get();
        if (id == null) return null;

        var newControlStreamResource = getConnectedSystemsApiClient().getControlStreamById(id, ResourceFormat.JSON, true).get();
        if (newControlStreamResource == null) return null;

        OSHControlStream controlStream = new OSHControlStream(this, id, newControlStreamResource);
        controlStreams.add(controlStream);
        notifyControlStreamAdded(controlStream);
        return controlStream;
    }

    /**
     * Delete a control stream associated with the system.
     *
     * @param controlStream The control stream to delete.
     * @return True if the deletion was successful, false otherwise.
     */
    public boolean deleteControlStream(OSHControlStream controlStream) throws ExecutionException, InterruptedException {
        Integer response = getConnectedSystemsApiClientExtras().deleteControlStream(controlStream.getId()).get();
        boolean success = response != null && response >= 200 && response < 300;

        if (success) {
            controlStreams.remove(controlStream);
            notifyControlStreamRemoved(controlStream);
        }

        return success;
    }

    /**
     * Get the endpoint for the data streams of this system.
     *
     * @return The endpoint.
     */
    public String getDataStreamsEndpoint() {
        return Utilities.joinPath(parentNode.getSystemsEndpoint(), systemResource.getId(), Service.DATASTREAMS.getEndpoint());
    }

    /**
     * Get the endpoint for the control streams of this system.
     *
     * @return The endpoint.
     */
    public String getControlStreamsEndpoint() {
        return Utilities.joinPath(parentNode.getSystemsEndpoint(), systemResource.getId(), Service.CONTROLSTREAMS.getEndpoint());
    }

    /**
     * Get the ID of the system.
     *
     * @return The ID.
     */
    public String getId() {
        return systemResource.getId();
    }

    /**
     * Get a list of discovered data streams associated with the system.
     *
     * @return The data streams.
     */
    public List<OSHDataStream> getDataStreams() {
        return List.copyOf(dataStreams);
    }

    /**
     * Get a list of discovered control streams associated with the system.
     *
     * @return The control streams.
     */
    public List<OSHControlStream> getControlStreams() {
        return List.copyOf(controlStreams);
    }

    public ConSysApiClient getConnectedSystemsApiClient() {
        return parentNode.getConnectedSystemsApiClient();
    }

    public ConSysApiClientExtras getConnectedSystemsApiClientExtras() {
        return parentNode.getConnectedSystemsApiClientExtras();
    }

    /**
     * Add a listener for data stream notifications.
     *
     * @param listener The listener.
     */
    public void addDataStreamNotificationListener(INotificationDataStream listener) {
        dataStreamNotificationListeners.add(listener);
    }

    /**
     * Remove a listener for data stream notifications.
     *
     * @param listener The listener.
     */
    public void removeDataStreamNotificationListener(INotificationDataStream listener) {
        dataStreamNotificationListeners.remove(listener);
    }

    /**
     * Notify listeners of a new data stream.
     *
     * @param dataStream The data stream.
     */
    public void notifyDataStreamAdded(OSHDataStream dataStream) {
        for (INotificationDataStream listener : dataStreamNotificationListeners) {
            listener.onItemAdded(dataStream);
        }
    }

    /**
     * Notify listeners of a removed data stream.
     *
     * @param dataStream The data stream.
     */
    public void notifyDataStreamRemoved(OSHDataStream dataStream) {
        for (INotificationDataStream listener : dataStreamNotificationListeners) {
            listener.onItemRemoved(dataStream);
        }
    }

    /**
     * Add a listener for control stream notifications.
     *
     * @param listener The listener.
     */
    public void addControlStreamNotificationListener(INotificationControlStream listener) {
        controlStreamNotificationListeners.add(listener);
    }

    /**
     * Remove a listener for control stream notifications.
     *
     * @param listener The listener.
     */
    public void removeControlStreamNotificationListener(INotificationControlStream listener) {
        controlStreamNotificationListeners.remove(listener);
    }

    /**
     * Notify listeners of a new control stream.
     *
     * @param controlStream The control stream.
     */
    public void notifyControlStreamAdded(OSHControlStream controlStream) {
        for (INotificationControlStream listener : controlStreamNotificationListeners) {
            listener.onItemAdded(controlStream);
        }
    }

    /**
     * Notify listeners of a removed control stream.
     *
     * @param controlStream The control stream.
     */
    public void notifyControlStreamRemoved(OSHControlStream controlStream) {
        for (INotificationControlStream listener : controlStreamNotificationListeners) {
            listener.onItemRemoved(controlStream);
        }
    }

    /**
     * The node this system belongs to.
     */
    public OSHNode getParentNode() {
        return parentNode;
    }

    /**
     * The system resource.
     */
    public ISystemWithDesc getSystemResource() {
        return systemResource;
    }

    /**
     * Set the system resource.
     * Used by the OSHNode to update the resource when it is rediscovered.
     */
    protected void setSystemResource(ISystemWithDesc systemResource) {
        this.systemResource = systemResource;
    }
}
