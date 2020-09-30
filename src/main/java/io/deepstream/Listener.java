package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

class Listener implements ResubscribeNotifier.ResubscribeListener {

    private final Topic topic;
    private final DeepstreamConfig deepstreamConfig;
    private final ResubscribeNotifier resubscribeNotifier;
    private AckTimeoutRegistry ackTimeoutRegistry;
    private String pattern;
    private ListenListener listenerCallback;
    private AbstractDeepstreamClient client;
    private Connection connection;

    @ObjectiveCName("init:pattern:listenerCallback:deepstreamConfig:client:connection:")
    public Listener(Topic topic,
                    String pattern,
                    ListenListener listenerCallback,
                    DeepstreamConfig deepstreamConfig,
                    AbstractDeepstreamClient client,
                    Connection connection) {
        this.topic = topic;
        this.pattern = pattern;
        this.listenerCallback = listenerCallback;
        this.deepstreamConfig = deepstreamConfig;
        this.client = client;
        this.connection = connection;
        this.resubscribeNotifier = new ResubscribeNotifier(this.client, this);
        this.ackTimeoutRegistry = client.getAckTimeoutRegistry();
    }

    /**
     * Send the listen request to the server
     */
    void start() {
        scheduleAckTimeout();
        sendListen();
    }

    public void destroy() {
        connection.sendMsg(topic, Actions.UNLISTEN, new String[]{pattern});
        resubscribeNotifier.destroy();
        listenerCallback = null;
        pattern = null;
        client = null;
        connection = null;
        ackTimeoutRegistry = null;
    }

    @ObjectiveCName("onMessage:")
    public void onMessage(final Message message) {
        if (message.action == Actions.ACK) {
            ackTimeoutRegistry.clear(message);
            return;
        }

        if (message.action == Actions.SUBSCRIPTION_FOR_PATTERN_FOUND) {
            boolean accepted = listenerCallback.onSubscriptionForPatternAdded(message.data[1]);
            if (accepted) {
                sendAccept(message.data[1]);
            } else {
                sendReject(message.data[1]);
            }
        } else if (message.action == Actions.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
            listenerCallback.onSubscriptionForPatternRemoved(message.data[1]);
        }
    }

    private void sendListen() {
        connection.sendMsg(topic, Actions.LISTEN, new String[]{pattern});
    }

    @ObjectiveCName("sendAccept:")
    private void sendAccept(String subscription) {
        connection.sendMsg(topic, Actions.LISTEN_ACCEPT, new String[]{pattern, subscription});
    }

    @ObjectiveCName("sendReject:")
    private void sendReject(String subscription) {
        connection.sendMsg(topic, Actions.LISTEN_REJECT, new String[]{pattern, subscription});
    }

    private void scheduleAckTimeout() {
        int subscriptionTimeout = deepstreamConfig.getSubscriptionTimeout();
        ackTimeoutRegistry.add(topic, Actions.LISTEN, pattern, subscriptionTimeout);
    }

    @Override
    public void resubscribe() {
        scheduleAckTimeout();
        sendListen();
    }
}
