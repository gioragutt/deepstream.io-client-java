package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The entry point for events, such as {@link EventHandler#subscribe(String, EventListener)},
 * {@link EventHandler#emit(String)} and provider functionality such as {@link EventHandler#listen(String, ListenListener)}
 */
public class EventHandler {
    private final EventEmitter<EventListener> emitter = new EventEmitter<>();
    private final Map<String, Listener> listeners = new HashMap<>();
    private final List<String> subscriptions = new ArrayList<>();

    private final int subscriptionTimeout;
    private final DeepstreamConfig deepstreamConfig;
    private final IConnection connection;
    private final AbstractDeepstreamClient client;
    private final AckTimeoutRegistry ackTimeoutRegistry;

    EventHandler(DeepstreamConfig deepstreamConfig, final IConnection connection, AbstractDeepstreamClient client) {
        this.subscriptionTimeout = deepstreamConfig.getSubscriptionTimeout();
        this.connection = connection;
        this.client = client;
        this.deepstreamConfig = deepstreamConfig;
        this.ackTimeoutRegistry = client.getAckTimeoutRegistry();

        new ResubscribeNotifier(this.client, () -> {
            for (String eventName : subscriptions) {
                connection.sendMsg(Topic.EVENT, Actions.SUBSCRIBE, new String[]{eventName});
            }
        });
    }

    /**
     * Subscribes to eventName and notifies the listener via {@link EventListener}
     * whenever it occurs locally or remotely
     *
     * @param eventName     The event name
     * @param eventListener The eventListener
     */
    @ObjectiveCName("subscribe:eventListener:")
    public void subscribe(String eventName, EventListener eventListener) {
        // TODO: wtf?
        if (emitter.hasListeners(eventName)) {
            subscriptions.add(eventName);
            ackTimeoutRegistry.add(Topic.EVENT, Actions.SUBSCRIBE, eventName, subscriptionTimeout);
            connection.send(MessageBuilder.getMsg(Topic.EVENT, Actions.SUBSCRIBE, eventName));
        }
        emitter.on(eventName, eventListener);
    }

    /**
     * Removes the listener added via {@link EventHandler}
     *
     * @param eventName     The event name
     * @param eventListener The listener that was previous added
     */
    @ObjectiveCName("unsubscribe:eventListener:")
    public void unsubscribe(String eventName, EventListener eventListener) {
        subscriptions.remove(eventName);
        emitter.off(eventName, eventListener);

        if (emitter.hasListeners(eventName)) {
            ackTimeoutRegistry.add(Topic.EVENT, Actions.UNSUBSCRIBE, eventName, subscriptionTimeout);
            connection.send(MessageBuilder.getMsg(Topic.EVENT, Actions.UNSUBSCRIBE, eventName));
        }
    }

    /**
     * @see EventHandler
     */
    @ObjectiveCName("emit:")
    public void emit(String eventName) {
        this.connection.send(MessageBuilder.getMsg(
                Topic.EVENT,
                Actions.EVENT,
                eventName));

        this.broadcastEvent(eventName, null);
    }

    /**
     * Emit an event that you want all subscribers, both local and remote to be informed of. You
     * can provide any object that can be serialised to json
     *
     * @param eventName the event name
     * @param data      the data to serialise and send with the event
     */
    @ObjectiveCName("emit:data:")
    public void emit(String eventName, Object data) {
        this.connection.send(MessageBuilder.getMsg(
                Topic.EVENT,
                Actions.EVENT,
                eventName,
                MessageBuilder.typed(data)));

        this.broadcastEvent(eventName, data);
    }

    /**
     * Listen to any subscriptions that have been made on the system that match the provided
     * pattern. If that subscription is found it will give the client the opportunity to accept
     * the role of being the provider, or rejecting it to pass responsibility to someone else.
     *
     * @param pattern        The pattern to match, must be valid regex
     * @param listenListener The listener to inform the client when a subscription
     *                       has been found or removed
     */
    @ObjectiveCName("listen:listenListener:")
    public void listen(String pattern, ListenListener listenListener) {
        if (listeners.get(pattern) != null) {
            client.onError(Topic.EVENT, Event.LISTENER_EXISTS, pattern);
            return;
        }

        synchronized (this) {
            Listener eventListener = new Listener(
                    Topic.EVENT,
                    pattern,
                    listenListener,
                    deepstreamConfig,
                    client,
                    connection);

            listeners.put(pattern, eventListener);
            eventListener.start();
        }
    }

    /**
     * Remove the listener added via {@link EventHandler}, this will remove
     * the provider as the active provider and allow another provider to take its place
     *
     * @param pattern The pattern that has been previously listened to
     */
    @ObjectiveCName("unlisten:")
    public void unlisten(String pattern) {
        Listener listener = listeners.get(pattern);
        if (listener != null) {
            ackTimeoutRegistry.add(Topic.EVENT, Actions.UNLISTEN, pattern, subscriptionTimeout);
            listener.destroy();
            listeners.remove(pattern);
        } else {
            client.onError(Topic.EVENT, Event.NOT_LISTENING, pattern);
        }
    }

    protected void handle(Message message) {
        String eventName;

        if (message.action == Actions.ACK) {
            eventName = message.data[1];
        } else {
            eventName = message.data[0];
        }

        if (message.action == Actions.EVENT) {
            if (message.data.length == 2) {
                broadcastEvent(eventName, MessageParser.convertTyped(
                        message.data[1],
                        client,
                        deepstreamConfig.getJsonParser()));
            } else {
                broadcastEvent(eventName, null);
            }
        } else if (this.listeners.get(eventName) != null) {
            this.listeners.get(eventName).onMessage(message);
        } else if (message.action == Actions.ACK) {
            this.ackTimeoutRegistry.clear(message);
        } else if (message.action == Actions.ERROR) {
            this.client.onError(Topic.EVENT, Event.getEvent(message.data[0]), message.data[1]);
        } else {
            this.client.onError(Topic.EVENT, Event.UNSOLICITED_MESSAGE, eventName);
        }
    }

    private void broadcastEvent(String eventName, Object args) {
        for (EventListener listener : this.emitter.listeners(eventName)) {
            listener.onEvent(eventName, args);
        }
    }
}
