package io.deepstream;


import com.google.gson.*;
import com.google.j2objc.annotations.ObjectiveCName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class represents a single record - an observable
 * dataset returned by {@link RecordHandler#getRecord(String)}
 */
public class Record {
    private static final String ALL_EVENT = "ALL_EVENT";
    private final ResubscribeNotifier resubscribeNotifier;
    private final SingleNotifier recordSetNotifier;
    private final AckTimeoutRegistry ackTimeoutRegistry;
    private final IConnection connection;
    private final AbstractDeepstreamClient client;
    private final Gson gson;
    private final JsonPath path;
    private final Set<RecordChangedCallback> allEventsSubscribers = ConcurrentHashMap.newKeySet();
    private final EventEmitter<RecordPathChangedCallback> pathChangedSubscribers = new EventEmitter<>();
    private final java.util.List<RecordEventsListener> recordEventsListeners = new ArrayList<>();
    private final java.util.List<Record.RecordDestroyPendingListener> recordDestroyPendingListeners = new ArrayList<>();
    private final java.util.List<RecordReadyListener> onceRecordReadyListeners = new ArrayList<>();
    private final String name;
    private final DeepstreamConfig deepstreamConfig;
    private final AtomicInteger usages = new AtomicInteger();
    private final ReentrantLock readyLock = new ReentrantLock();
    private volatile boolean isReady = false;
    private volatile boolean isDestroyed = false;
    private boolean isDiscarded = false;
    private int version = -1;
    private RecordMergeStrategy mergeStrategy;
    private RecordRemoteUpdateHandler recordRemoteUpdateHandler;
    private JsonElement data = new JsonObject();
    private boolean hasProvider = false;

    /**
     * Constructor is not public since it is created via {@link RecordHandler#getRecord(String)}
     *
     * @param name             The unique name of the record
     * @param recordOptions    A map of deepstreamConfig, e.g. { persist: true }
     * @param connection       The instance of the server connection
     * @param deepstreamConfig Deepstream deepstreamConfig
     * @param client           deepstream.io client
     */
    @ObjectiveCName("init:recordOptions:connection:deepstreamConfig:client:")
    Record(String name,
           Map<?, ?> recordOptions,
           IConnection connection,
           DeepstreamConfig deepstreamConfig,
           AbstractDeepstreamClient client) {
        this.name = name;
        this.deepstreamConfig = deepstreamConfig;
        this.connection = connection;
        this.client = client;

        ackTimeoutRegistry = client.getAckTimeoutRegistry();
        gson = deepstreamConfig.getJsonParser();
        path = new JsonPath(data);
        if (deepstreamConfig.getRecordMergeStrategy() != null) {
            mergeStrategy = RecordMergeStrategies.INSTANCE.getMergeStrategy(
                    deepstreamConfig.getRecordMergeStrategy());
        }

        resubscribeNotifier = new ResubscribeNotifier(client, this::sendRead);
        recordSetNotifier = new SingleNotifier(
                client, connection, Topic.RECORD, Actions.PATCH, deepstreamConfig.getSubscriptionTimeout());
    }

    /**
     * Send the subscriber request to the server
     */
    void start() {
        scheduleAcks();
        sendRead();
    }

    /**
     * Return whether the record data has been loaded from the server
     *
     * @return true if record has been loaded
     */
    public boolean isReady() {
        return isReady;
    }

    /**
     * Return whether the record has an active provider
     *
     * @return true if record has been loaded
     */
    public boolean hasProvider() {
        return hasProvider;
    }

    /**
     * Return whether the record data has been destroyed. If true and you need to use the method create it again via
     * {@link RecordHandler#getRecord(String)}
     *
     * @return true if record has been destroyed
     */
    public boolean isDestroyed() {
        return isDestroyed;
    }

    /**
     * Return the record version. This is solely used within a {@link RecordMergeStrategy}.
     *
     * @return -1 if not loaded, otherwise the local version number
     */
    @ObjectiveCName("version")
    public int version() {
        return version;
    }

    /**
     * Return the record name
     *
     * @return The record name
     */
    public String name() {
        return name;
    }

    /**
     * Adds a Listener that will notify you if a Discard, Delete or Error event occurs
     *
     * @param recordEventsListener The listener to add
     * @return The record
     */
    @ObjectiveCName("addRecordEventsListener:")
    public Record addRecordEventsListener(RecordEventsListener recordEventsListener) {
        recordEventsListeners.add(recordEventsListener);
        return this;
    }

    /**
     * Remove listener added via {@link io.deepstream.Record#addRecordEventsListener(RecordEventsListener)}
     *
     * @param recordEventsListener The listener to remove
     * @return The record
     */
    @ObjectiveCName("removeRecordEventsListener:")
    public Record removeRecordEventsListener(RecordEventsListener recordEventsListener) {
        recordEventsListeners.remove(recordEventsListener);
        return this;
    }

    /**
     * Set a merge strategy that comes with deepstream. These are currently LOCAL_WINS and REMOTE_WINS
     *
     * @param mergeStrategy The name of the built in merge strategy to use
     * @return The record
     */
    @ObjectiveCName("setMergeStrategy:")
    public Record setMergeStrategy(MergeStrategy mergeStrategy) {
        this.mergeStrategy = RecordMergeStrategies.INSTANCE.getMergeStrategy(mergeStrategy);
        return this;
    }

    /**
     * Set a custom merge strategy for this record
     *
     * @param mergeStrategy The custom merge strategy to use
     * @return The record
     */
    @ObjectiveCName("setCustomMergeStrategy:")
    public Record setMergeStrategy(RecordMergeStrategy mergeStrategy) {
        this.mergeStrategy = mergeStrategy;
        return this;
    }

    /**
     * Gets the a class to represent the record Type.
     * This is currently not public as it is used for the list class, but we
     * should investigate the work done by AlexH to get this to be a more intuitive
     * API
     *
     * @return The object with the type passed in and containing the records data
     */
    <T> T get(Class<T> type) {
        return deepCopy(data, type);
    }

    /**
     * Gets the value at the path indicated.<br/>
     * <br/>
     * For example, if the record data is:<br/>
     * { "name": "Yasser", pets: [ { type: "Dog", "name": "Whiskey", age: 3} ]}<br/>
     * <br/>
     * We can do:<br/>
     * get( "name" ) -> {@link JsonElement#getAsString()}<br/>
     * get( "pets[0]" ) -> {@link JsonElement#getAsJsonObject()}<br/>
     * get( "pets[0].age") -> {@link JsonElement#getAsInt()}<br/>
     *
     * @return The record data as a JsonElement
     */
    @ObjectiveCName("get:")
    public JsonElement get(String path) {
        return deepCopy(this.path.get(path));
    }

    /**
     * Gets the entire record data and should always return a {@link JsonObject}, except when using
     * a {@link io.deepstream.List}, but then you should always be using it via {@link io.deepstream.List#getEntries()} ;)
     *
     * @return The record data as a json element
     * @see Record#get(String)
     */
    public JsonElement get() {
        return deepCopy(data);
    }

    /**
     * Set the value for the entire record<br/>
     * Make sure that the Object passed in can be serialised to a JsonElement, otherwise it will
     * throw a {@link IllegalStateException}. Best way to guarantee this is by setting Json friendly objects,
     * such as {@link Map}. Since this is a root the object should also not be a primitive.
     *
     * @throws DeepstreamRecordDestroyedException Thrown if the list has been destroyed and can't perform more actions
     * @see Record#set(String, Object)
     */
    @ObjectiveCName("set:")
    public Record set(JsonElement value) {
        return set(null, value, false);
    }

    /**
     * Set the value for a specific path in your Record data.<br/>
     * Make sure that the Object passed in can be serialised to a JsonElement, otherwise it will
     * throw a {@link IllegalStateException}.<br/>
     * The best way to guarantee this is by setting Json friendly objects,
     * such as {@link Map}.<br/>
     * If you path is not null, you can pass in primitives as long as the path
     * is not null, which is the equivalent of calling {@link Record#set(JsonElement)}.
     *
     * @param path  The path with the JsonElement at which to set the value
     * @param value The value to set
     * @return The record
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    @ObjectiveCName("set:value:")
    public Record set(String path, Object value) {
        return this.set(path, value, false);
    }

    /**
     * Set the value for the entire record synchronously and gives acknowledgement
     * whether there were any errors storing the record data in cache or storage.<br/>
     * Make sure that the Object passed in can be serialised to a JsonElement, otherwise it will
     * throw a {@link IllegalStateException}. Best way to guarantee this is by setting Json friendly objects,
     * such as {@link Map}. Since this is a root the object should also not be a primitive.
     *
     * @see Record#set(String, Object)
     */
    @ObjectiveCName("setWithAck:")
    public RecordSetResult setWithAck(Object value) {
        return this.setWithAck(null, value);
    }

    /**
     * Set the value for a specific path in your Record data synchronously and gives acknowledgement
     * whether there were any errors storing the record data in cache or storage.<br/>
     * Make sure that the Object passed in can be serialised to a JsonElement, otherwise it will
     * throw a {@link IllegalStateException}.<br/>
     * The best way to guarantee this is by setting Json friendly objects,
     * such as {@link Map}.<br/>
     * If your path is not null, you can pass in primitives, which is the
     * equivalent of calling {@link Record#set(String, Object)}.
     *
     * @param path  The path with the JsonElement at which to set the value
     * @param value The value to set
     * @return The record
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    @ObjectiveCName("setWithAck:value:")
    public RecordSetResult setWithAck(String path, Object value) {
        throwExceptionIfDestroyed("set");

        JsonElement element;
        if (value instanceof String) {
            element = new JsonPrimitive((String) value);
        } else if (value instanceof Number) {
            element = new JsonPrimitive((Number) value);
        } else if (value instanceof Boolean) {
            element = new JsonPrimitive((Boolean) value);
        } else {
            element = gson.toJsonTree(value);
        }

        JsonElement object = this.path.get(path);

        if (object != null && object.equals(value)) {
            return new RecordSetResult(null);
        } else if (path == null && this.data.equals(value)) {
            return new RecordSetResult(null);
        }

        final RecordSetResult[] result = new RecordSetResult[1];
        final Map<String, JsonElement> oldValues = beginChange();
        this.path.set(path, element);
        this.data = this.path.getCoreElement();

        JsonObject config = new JsonObject();
        config.addProperty("writeSuccess", true);
        String newVersion = String.valueOf(this.version + 1);

        String[] data;
        if (path == null) {
            data = new String[]{this.name(), newVersion, element.toString(), config.toString()};
        } else {
            data = new String[]{this.name(), newVersion, path, MessageBuilder.typed(value), config.toString()};
        }

        Actions action;
        if (path == null) {
            action = Actions.UPDATE;
        } else {
            action = Actions.PATCH;
        }

        final CountDownLatch snapshotLatch = new CountDownLatch(1);
        this.recordSetNotifier.request(newVersion, action, data, new SingleNotifier.SingleNotifierCallback() {
            @Override
            public void onSingleNotifierError(String name, DeepstreamError error) {
                result[0] = new RecordSetResult(error.getMessage());
                snapshotLatch.countDown();
            }

            @Override
            public void onSingleNotifierResponse(String name, Object data) {
                completeChange(oldValues);
                result[0] = new RecordSetResult(null);
                snapshotLatch.countDown();
            }
        });
        try {
            snapshotLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result[0];
    }

    /**
     * Notifies the user whenever anything under the path provided has changed.
     *
     * @see Record#subscribe(String, RecordPathChangedCallback, boolean)
     */
    @ObjectiveCName("subscribe:recordPathChangedCallback:")
    public Record subscribe(String path, RecordPathChangedCallback recordPathChangedCallback) {
        return subscribe(path, recordPathChangedCallback, false);
    }

    /**
     * Subscribe to record changes.<br/>
     * <p>
     * If a path is provided, updates will be based on everything in or under it.<br/>
     * <p>
     * If trigger now is true, the listener will be immediately fired with the current value.
     *
     * @param path                      The path to listen to
     * @param recordPathChangedCallback The listener to add
     * @param triggerNow                Whether to immediately trigger the listener with the current value
     * @return The record
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    @ObjectiveCName("subscribe:recordPathChangedCallback:triggerNow:")
    public Record subscribe(String path,
                            RecordPathChangedCallback recordPathChangedCallback,
                            boolean triggerNow) {
        throwExceptionIfDestroyed("subscribe");

        this.pathChangedSubscribers.on(path, recordPathChangedCallback);

        if (triggerNow) {
            recordPathChangedCallback.onRecordPathChanged(this.name, path, this.get(path));
        }

        return this;
    }

    /**
     * Notifies the user whenever anything inside the Record has changed.
     *
     * @see Record#subscribe(RecordChangedCallback, boolean)
     */
    @ObjectiveCName("subscribe:")
    public Record subscribe(RecordChangedCallback recordChangedCallback) {
        return subscribe(recordChangedCallback, false);
    }

    /**
     * Notifies the user whenever anything inside the Record has changed, and triggers the listener immediately.
     *
     * @param recordChangedCallback The callback for whenever anything within the record changes
     * @param triggerNow            Whether to call the callback immediately with the current value
     */
    @ObjectiveCName("subscribe:triggerNow:")
    public Record subscribe(RecordChangedCallback recordChangedCallback, boolean triggerNow) {
        throwExceptionIfDestroyed("subscribe");

        this.allEventsSubscribers.add(recordChangedCallback);

        if (triggerNow) {
            recordChangedCallback.onRecordChanged(this.name, this.get());
        }

        return this;
    }

    /**
     * Remove the listener added via {@link Record#subscribe(RecordChangedCallback, boolean)}
     *
     * @param recordChangedCallback The listener to remove
     * @return The record
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    @ObjectiveCName("unsubscribe:")
    public Record unsubscribe(RecordChangedCallback recordChangedCallback) {
        throwExceptionIfDestroyed("unsubscribe");
        this.allEventsSubscribers.remove(recordChangedCallback);
        return this;
    }

    /**
     * Remove the listener added via {@link Record#subscribe(String, RecordPathChangedCallback, boolean)}
     *
     * @param path                      The path to unsubscribe from
     * @param recordPathChangedCallback The listener to remove
     * @return The record
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    @ObjectiveCName("unsubscribe:recordPathChangedCallback:")
    public Record unsubscribe(String path, RecordPathChangedCallback recordPathChangedCallback) {
        throwExceptionIfDestroyed("unsubscribe");
        this.pathChangedSubscribers.off(path, recordPathChangedCallback);
        return this;
    }

    /**
     * Discard the record. This should be called whenever you are done with the record retrieved by {@link RecordHandler#getRecord(String)}.
     * This does not guarantee that your subscriptions have been unsubscribed, so make sure to do that first!<br/>
     * <p>
     * If all usages of the same record have been discarded, the record will no longer be updated from the server and
     * any further usages will require the record to be retrieved again via {@link RecordHandler#getRecord(String)}<br/>
     * <p>
     * Once the record is successfully discard, you can be notified via {@link RecordEventsListener#onRecordDiscarded(String)}
     *
     * @return The record
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    public Record discard() {
        throwExceptionIfDestroyed("discard");
        if (this.usages.decrementAndGet() <= 0) {
            finishDiscard();
        }
        return this;
    }

    void finishDiscard() {
        // This must be a synchronized block so that RecordHandler.getRecord will not continue until the
        // record has been removed from the cache in onDestroyPending.
        synchronized (this) {
            // We only want one thread to do the discard one time so check the isDiscarded flag within
            // this synchronized section.
            if (!isDiscarded) {
                whenReady((recordName, record) -> {
                    ackTimeoutRegistry.add(Topic.RECORD, Actions.UNSUBSCRIBE, name, deepstreamConfig.getSubscriptionTimeout());
                    connection.send(MessageBuilder.getMsg(Topic.RECORD, Actions.UNSUBSCRIBE, name));

                    for (RecordDestroyPendingListener recordDestroyPendingHandler : recordDestroyPendingListeners) {
                        recordDestroyPendingHandler.onDestroyPending(name);
                    }
                });
                destroy();
                isDiscarded = true;
            }
        }
    }

    /**
     * Delete the record. This is called when you want to remove the record entirely from deepstream, deleting it from storage
     * and cache and telling all other users that it has been deleted. This in turn will force all clients to discard the record.<br/>
     * <p>
     * Once the record is successfully deleted, you can be notified via {@link RecordEventsListener#onRecordDeleted(String)} (String)}</br>
     *
     * @return The record
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    @ObjectiveCName("delete")
    public Record delete() {
        throwExceptionIfDestroyed("delete");

        whenReady((recordName, record) -> {
            ackTimeoutRegistry.add(Topic.RECORD, Actions.DELETE, name, Event.DELETE_TIMEOUT, deepstreamConfig.getSubscriptionTimeout());
            connection.send(MessageBuilder.getMsg(Topic.RECORD, Actions.DELETE, name));

            for (RecordDestroyPendingListener recordDestroyPendingHandler : recordDestroyPendingListeners) {
                recordDestroyPendingHandler.onDestroyPending(name);
            }
        });


        return this;
    }

    /**
     * Add a recordReadyListener as a callback. This means it will be called once when the record is ready, either in sync
     * or async if the record is not already ready.
     *
     * @param recordReadyListener The recordReadyListener that will be triggered only **once**
     * @return The record
     */
    @ObjectiveCName("whenReady:")
    Record whenReady(RecordReadyListener recordReadyListener) {
        readyLock.lock();
        try {
            if (isReady) {
                readyLock.unlock();
                recordReadyListener.onRecordReady(name, this);
            } else {
                onceRecordReadyListeners.add(recordReadyListener);
            }
        } finally {
            if (readyLock.isHeldByCurrentThread()) {
                readyLock.unlock();
            }
        }
        return this;
    }

    /**
     * Invoked when a message is received from {@link RecordHandler#handle(Message)}
     *
     * @param message The message received from the server
     */
    @ObjectiveCName("onMessage:")
    void onMessage(Message message) {
        if (message.action == Actions.ACK) {
            processAckMessage(message);
        } else if (message.action == Actions.READ && this.version() == -1) {
            onRead(message);
        } else if (message.action == Actions.READ || message.action == Actions.UPDATE || message.action == Actions.PATCH) {
            applyUpdate(message);
        } else if (message.action == Actions.WRITE_ACKNOWLEDGEMENT) {
            handleWriteAcknowledgement(message);
        } else if (message.action == Actions.SUBSCRIPTION_HAS_PROVIDER) {
            updateHasProvider(message);
        } else if (message.data[0].equals(Event.VERSION_EXISTS.toString())) {
            recoverRecord(Integer.parseInt(message.data[2]), gson.fromJson(message.data[3], JsonElement.class));
        } else if (message.data[0].equals(Event.MESSAGE_DENIED.toString())) {
            clearTimeouts();
        }
    }

    private void handleWriteAcknowledgement(Message message) {
        String val = String.valueOf(message.data[1]);
        JsonArray versions = gson.fromJson(val, JsonArray.class);
        Object error = MessageParser.convertTyped(message.data[2], this.client, gson);
        if (error != null) {
            this.recordSetNotifier.receive(versions, new DeepstreamError((String) error));
        } else {
            this.recordSetNotifier.receive(versions, null);
        }
    }

    /**
     * This gives us a handle to before and after a record is updated remotely. This is currently used by {@link io.deepstream.List}
     *
     * @param recordRemoteUpdateHandler The listener to notify before and after an update is applied
     */
    @ObjectiveCName("setRecordRemoteUpdateHandler:")
    void setRecordRemoteUpdateHandler(RecordRemoteUpdateHandler recordRemoteUpdateHandler) {
        this.recordRemoteUpdateHandler = recordRemoteUpdateHandler;
    }

    @ObjectiveCName("updateHasProvider:")
    private void updateHasProvider(Message message) {
        this.hasProvider = (Boolean) MessageParser.convertTyped(message.data[1], client, gson);
        for (RecordEventsListener recordEventsListener : recordEventsListeners) {
            recordEventsListener.onRecordHasProviderChanged(name, hasProvider);
        }
    }

    /**
     * Apply the message received on the server on the record
     */
    @ObjectiveCName("applyUpdate:")
    private void applyUpdate(Message message) {
        int newVersion = Integer.parseInt(message.data[1]);

        JsonElement data;
        boolean delete = false;
        if (message.action == Actions.PATCH) {
            Object rawData = MessageParser.convertTyped(message.data[3], client, gson);
            if (rawData == Types.UNDEFINED) {
                delete = true;
                data = null;
            } else {
                data = gson.toJsonTree(rawData);
            }
        } else {
            data = gson.fromJson(message.data[2], JsonElement.class);
        }

        if (version != -1 && version + 1 != newVersion) {
            if (message.action == Actions.PATCH) {
                // Request a snapshot so that a merge can be done with the read reply which contains
                // the full state of the record
                connection.send(MessageBuilder.getMsg(Topic.RECORD, Actions.SNAPSHOT, name));
            } else {
                recoverRecord(newVersion, data);
            }
            return;
        }

        if (recordRemoteUpdateHandler != null) {
            recordRemoteUpdateHandler.beforeRecordUpdate();
        }

        Map<String, JsonElement> oldValues = beginChange();

        version = newVersion;
        if (message.action == Actions.PATCH) {
            if (delete) {
                path.delete(message.data[2]);
            } else {
                path.set(message.data[2], data);
            }
        } else {
            setData(data);
        }

        completeChange(oldValues);

        if (recordRemoteUpdateHandler != null) {
            recordRemoteUpdateHandler.afterRecordUpdate();
        }
    }

    private void setData(JsonElement data) {
        this.data = data;
        path.setCoreElement(data);
    }

    /**
     * Called when a merge conflict is detected by a VERSION_EXISTS error or if an update received
     * is directly after the clients. If no merge strategy is configure it will emit a VERSION_EXISTS
     * error and the record will remain in an inconsistent state.
     *
     * @param remoteVersion The remote version number
     * @param remoteData    The remote object data
     */
    @ObjectiveCName("recoverRecord:remoteData:")
    private void recoverRecord(int remoteVersion, JsonElement remoteData) {
        try {
            JsonElement mergedData = mergeStrategy.merge(this, remoteData, remoteVersion);
            version = remoteVersion;
            set(null, mergedData, true);
        } catch (RecordMergeStrategyException ex) {
            client.onError(
                    Topic.RECORD,
                    Event.VERSION_EXISTS,
                    String.format("Received update for %d but version is %d", remoteVersion, version));
        }
    }

    private void scheduleAcks() {
        ackTimeoutRegistry.add(
                Topic.RECORD,
                Actions.SUBSCRIBE,
                name,
                Event.ACK_TIMEOUT,
                deepstreamConfig.getRecordReadAckTimeout());

        ackTimeoutRegistry.add(
                Topic.RECORD,
                Actions.READ,
                name,
                Event.RESPONSE_TIMEOUT,
                deepstreamConfig.getRecordReadTimeout());
    }

    private void clearTimeouts() {
        ackTimeoutRegistry.clear(Topic.RECORD, Actions.SUBSCRIBE, name);
        ackTimeoutRegistry.clear(Topic.RECORD, Actions.READ, name);
    }

    /**
     * First of two steps that are called for incoming and outgoing updates.
     * Saves the current value of all paths the app is subscribed to.
     *
     * @return The record
     */
    private Map<String, JsonElement> beginChange() {
        Map<String, JsonElement> oldValues = new HashMap<>();

        for (String path : pathChangedSubscribers.getEvents()) {
            oldValues.put(path, get(path));
        }

        if (!allEventsSubscribers.isEmpty()) {
            oldValues.put(ALL_EVENT, get());
        }

        if (oldValues.isEmpty()) {
            return null;
        }
        return oldValues;
    }

    /**
     * Second of two steps that are called for incoming and outgoing updates.
     * Compares the new values for every path with the previously stored ones and
     * updates the subscribers if the value has changed
     *
     * @param oldValues The previous paths and values
     */
    // @ObjectiveCName("completeChange:")
    private void completeChange(Map<String, JsonElement> oldValues) {
        if (oldValues == null || oldValues.isEmpty()) {
            return;
        }

        JsonElement oldValue = oldValues.remove(ALL_EVENT);
        if (oldValue != null && !oldValue.equals(data)) {
            for (RecordChangedCallback listener : allEventsSubscribers) {
                listener.onRecordChanged(name, get());
            }
        }

        for (String path : oldValues.keySet()) {
            oldValue = oldValues.get(path);
            JsonElement newValue = get(path);
            if (oldValue == null || !oldValue.equals(newValue)) {
                for (RecordPathChangedCallback listener : pathChangedSubscribers.listeners(path)) {
                    listener.onRecordPathChanged(name, path, newValue);
                }
            }
        }
    }

    /**
     * Throw an exception if the record has been destroyed
     *
     * @param method The method to call
     * @throws DeepstreamRecordDestroyedException Thrown if the record has been destroyed and can't perform more actions
     */
    // @ObjectiveCName("throwExceptionIfDestroyed:")
    private void throwExceptionIfDestroyed(String method) {
        if (isDestroyed) {
            throw new DeepstreamRecordDestroyedException(method);
        }
    }

    /**
     * @param message The ack {@link Message}
     */
    @ObjectiveCName("processAckMessage:")
    private void processAckMessage(Message message) {
        Actions action = Actions.getAction(message.data[0]);
        ackTimeoutRegistry.clear(message);

        if (action.equals(Actions.DELETE)) {
            for (RecordEventsListener recordEventsListener : recordEventsListeners) {
                recordEventsListener.onRecordDeleted(name);
            }
            destroy();
        } else if (action.equals(Actions.UNSUBSCRIBE)) {
            for (RecordEventsListener recordEventsListener : recordEventsListeners) {
                recordEventsListener.onRecordDiscarded(name);
            }
            destroy();
        }
    }

    /**
     * Callback for incoming read messages
     *
     * @param message The read {@link Message}
     */
    @ObjectiveCName("onRead:")
    private void onRead(Message message) {
        ackTimeoutRegistry.clear(message);

        Map<String, JsonElement> oldValues = beginChange();
        version = Integer.parseInt(message.data[1]);
        setData(MessageParser.readJsonStream(message.data[2], gson));
        completeChange(oldValues);
        setReady();
    }

    /**
     * Invokes method calls that where queued while the record wasn't ready
     * and emits the ready event
     */
    private void setReady() {
        ArrayList<RecordReadyListener> listCopy;

        readyLock.lock();
        try {
            isReady = true;
            // Capture the list inside the lock so we can execute the listeners outside the lock.
            listCopy = new ArrayList<>(onceRecordReadyListeners);
            onceRecordReadyListeners.clear();
        } finally {
            readyLock.unlock();
        }

        for (RecordReadyListener recordReadyListener : listCopy) {
            recordReadyListener.onRecordReady(name, this);
        }
    }

    /**
     * Sends the read message, either initially at record
     * creation or after a lost connection has been re-established
     */
    private void sendRead() {
        if (client.getConnectionState() == ConnectionState.OPEN) {
            connection.send(MessageBuilder.getMsg(Topic.RECORD, Actions.CREATEORREAD, name));
        }
    }

    /**
     * Send the update to the server, either as an update or patch
     *
     * @param path  The path to update if a patch
     * @param value The value to update the record with
     */
    @ObjectiveCName("sendUpdate:value:")
    private void sendUpdate(String path, Object value) {
        version++;
        if (path == null || path.equals("")) {
            String[] payload = {name, String.valueOf(version), gson.toJson(value)};
            connection.sendMsg(Topic.RECORD, Actions.UPDATE, payload);
        } else {
            String[] payload = {name, String.valueOf(version), path, MessageBuilder.typed(value)};
            connection.sendMsg(Topic.RECORD, Actions.PATCH, payload);
        }
    }

    /**
     * Destroys the record and nulls all
     * its dependencies
     */
    private void destroy() {
        clearTimeouts();
        resubscribeNotifier.destroy();
        recordSetNotifier.destroy();
        isReady = false;
        isDestroyed = true;
    }

    /**
     * Generate a deep copy of the object to prevent user to modify record data directly
     */
    @ObjectiveCName("deepCopy:")
    private JsonElement deepCopy(JsonElement element) {
        try {
            return deepCopy(element, JsonElement.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Generate a deep copy of the object and cast it to a class of any type, used by {@link io.deepstream.List}
     */
    private <T> T deepCopy(JsonElement element, Class<T> type) {
        return gson.fromJson(gson.toJson(element, JsonElement.class), type);
    }

    /**
     * This forces an update, which is useful when trying to reconcile a merge conflict when the merge is the same
     * but the version number isn't.
     */
    @ObjectiveCName("set:value:force:")
    private Record set(String path, Object value, boolean force) {
        throwExceptionIfDestroyed("set");

        JsonElement element;
        if (value instanceof String) {
            element = new JsonPrimitive((String) value);
        } else if (value instanceof Number) {
            element = new JsonPrimitive((Number) value);
        } else if (value instanceof Boolean) {
            element = new JsonPrimitive((Boolean) value);
        } else {
            element = gson.toJsonTree(value);
        }

        JsonElement object = this.path.get(path);

        if (!force) {
            if (object != null && object.equals(value)) {
                return this;
            } else if (path == null && data.equals(value)) {
                return this;
            }
        }

        Map<String, JsonElement> oldValues = beginChange();
        this.path.set(path, element);
        data = this.path.getCoreElement();
        sendUpdate(path, value);
        completeChange(oldValues);

        return this;
    }

    /**
     * Add a destroy pending listener, used by the RecordHandler and potentially other internal stores
     */
    @ObjectiveCName("addRecordDestroyPendingListener:")
    void addRecordDestroyPendingListener(RecordDestroyPendingListener recordDestroyPendingListener) {
        recordDestroyPendingListeners.add(recordDestroyPendingListener);
    }

    int getAndIncrementUsage() {
        return usages.getAndIncrement();
    }

    @ObjectiveCName("RecordRemoteUpdateHandler")
    interface RecordRemoteUpdateHandler {
        /**
         * Called before a remote update is applied to the current data
         */
        @ObjectiveCName("beforeRecordUpdate")
        void beforeRecordUpdate();

        /**
         * Called after a remote update is applied to the current data
         */
        @ObjectiveCName("afterRecordUpdate")
        void afterRecordUpdate();
    }

    @ObjectiveCName("RecordDestroyPendingListener")
    interface RecordDestroyPendingListener {
        /**
         * Called whenever the client is about to send the server a {@link Record#discard()} or {@link Record#delete()} event.<br/>
         * This should not be required to be implemented
         *
         * @param recordName The name of the record being destroyed
         */
        @ObjectiveCName("onDestroyPending:")
        void onDestroyPending(String recordName);
    }

    /**
     * A listener that notifies the user whenever the record state is ready.
     */
    @ObjectiveCName("RecordReadyListener")
    interface RecordReadyListener {
        /**
         * Called when the record is loaded from the server
         *
         * @param recordName The name of the record which is now ready
         * @param record     The record which is now ready / loaded from server
         */
        @ObjectiveCName("onRecordReady:record:")
        void onRecordReady(String recordName, Record record);
    }
}
