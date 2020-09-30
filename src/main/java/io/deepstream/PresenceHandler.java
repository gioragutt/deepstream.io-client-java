package io.deepstream;


import com.google.j2objc.annotations.ObjectiveCName;

public class PresenceHandler {
    private static final String EVENT_NAME = Topic.PRESENCE.toString();
    private static final String[] SUBSCRIBE_PAYLOAD = {Actions.SUBSCRIBE.toString()};

    private final EventEmitter<PresenceEventListener> emitter = new EventEmitter<>();
    private final int subscriptionTimeout;
    private final Connection connection;
    private final AbstractDeepstreamClient client;
    private final AckTimeoutRegistry ackTimeoutRegistry;
    private final SingleNotifier notifier;

    PresenceHandler(DeepstreamConfig deepstreamConfig,
                    Connection connection,
                    AbstractDeepstreamClient client) {
        this.subscriptionTimeout = deepstreamConfig.getSubscriptionTimeout();
        this.connection = connection;
        this.client = client;
        this.ackTimeoutRegistry = client.getAckTimeoutRegistry();

        notifier = new SingleNotifier(
                client,
                connection,
                Topic.PRESENCE,
                Actions.QUERY,
                subscriptionTimeout);

        new ResubscribeNotifier(this.client, () -> {
            if (!emitter.listeners(EVENT_NAME).isEmpty()) {
                connection.sendMsg(Topic.PRESENCE, Actions.SUBSCRIBE, SUBSCRIBE_PAYLOAD);
            }
        });
    }

    /**
     * Queries for clients logged into deepstream
     *
     * @return List<String> a list of currently connected clients
     * @throws DeepstreamError
     */
    public String[] getAll() throws DeepstreamError {

        SingleNotifier.CommonRequestCallback<String[]> callback =
                new SingleNotifier.CommonRequestCallback<>();

        notifier.request(Actions.QUERY.toString(), callback);

        callback.waitForResponse();
        callback.throwIfError();
        return callback.data();
    }

    /**
     * Subscribes to clients login/logout events
     *
     * @param eventListener The listener that will be called with the username of the client
     *                      and a boolean to indicated whether they logged in or out
     */
    @ObjectiveCName("subscribe:")
    public void subscribe(PresenceEventListener eventListener) {
        if (this.emitter.hasListeners(EVENT_NAME)) {
            this.ackTimeoutRegistry.add(Topic.PRESENCE, Actions.SUBSCRIBE, Topic.PRESENCE.toString(), this.subscriptionTimeout);
            this.connection.send(MessageBuilder.getMsg(Topic.PRESENCE, Actions.SUBSCRIBE, Actions.SUBSCRIBE.toString()));
        }
        this.emitter.on(EVENT_NAME, eventListener);
    }

    /**
     * Removes the listener added via {@link PresenceHandler}
     *
     * @param eventListener The listener that will be called with the username of the client
     *                      and a boolean to indicated whether they logged in or out
     */
    @ObjectiveCName("unsubscribe:")
    public void unsubscribe(PresenceEventListener eventListener) {
        this.emitter.off(EVENT_NAME, eventListener);
        if (this.emitter.hasListeners(EVENT_NAME)) {
            this.ackTimeoutRegistry.add(Topic.PRESENCE, Actions.UNSUBSCRIBE, Topic.PRESENCE.toString(), this.subscriptionTimeout);
            this.connection.send(MessageBuilder.getMsg(Topic.PRESENCE, Actions.UNSUBSCRIBE, Actions.UNSUBSCRIBE.toString()));
        }
    }


    protected void handle(Message message) {
        if (message.action == Actions.ERROR && message.data[0].equals(Event.MESSAGE_DENIED.toString())) {
            this.ackTimeoutRegistry.clear(message);
            this.client.onError(Topic.PRESENCE, Event.MESSAGE_DENIED, message.data[1]);
            return;
        }

        switch (message.action) {
            case ACK:
                this.ackTimeoutRegistry.clear(message);
                break;
            case PRESENCE_JOIN:
                this.broadcastEvent(true, message.data[0]);
                break;
            case PRESENCE_LEAVE:
                this.broadcastEvent(false, message.data[0]);
                break;
            case QUERY:
                this.notifier.receive(Actions.QUERY.toString(), null, message.data);
                break;
            default:
                this.client.onError(Topic.PRESENCE, Event.UNSOLICITED_MESSAGE, message.action.toString());
                break;
        }
    }

    private void broadcastEvent(boolean isLoginEvent, String data) {
        for (PresenceEventListener listener : emitter.listeners(EVENT_NAME)) {
            if (isLoginEvent)
                listener.onClientLogin(data);
            else
                listener.onClientLogout(data);
        }
    }
}
