package io.deepstream;

import com.google.gson.JsonElement;
import com.google.j2objc.annotations.ObjectiveCName;

abstract class AbstractDeepstreamClient {
    private final AckTimeoutRegistry ackTimeoutRegistry = new AckTimeoutRegistry(this);
    private DeepstreamRuntimeErrorHandler deepstreamRuntimeErrorHandler;

    @ObjectiveCName("addConnectionChangeListener:")
    abstract AbstractDeepstreamClient addConnectionChangeListener(ConnectionStateListener connectionStateListener);

    @ObjectiveCName("removeConnectionChangeListener:")
    abstract AbstractDeepstreamClient removeConnectionChangeListener(ConnectionStateListener connectionStateListener);

    abstract ConnectionState getConnectionState();

    abstract LoginResult login();

    @ObjectiveCName("login:")
    abstract LoginResult login(JsonElement data);

    abstract AbstractDeepstreamClient close();

    abstract String getUid();

    AckTimeoutRegistry getAckTimeoutRegistry() {
        return ackTimeoutRegistry;
    }

    /**
     * Adds a {@link DeepstreamRuntimeErrorHandler} that will catch all RuntimeErrors such as AckTimeouts and allow
     * the user to gracefully handle them.
     *
     * @param deepstreamRuntimeErrorHandler The listener to set
     */
    @ObjectiveCName("setRuntimeErrorHandler:")
    public void setRuntimeErrorHandler(DeepstreamRuntimeErrorHandler deepstreamRuntimeErrorHandler) {
        this.deepstreamRuntimeErrorHandler = deepstreamRuntimeErrorHandler;
    }

    @ObjectiveCName("onError:event:msg:")
    void onError(Topic topic, Event event, String msg) throws DeepstreamException {
        /*
         * Help to diagnose the problem quicker by checking for
         * some mon problems
         */
        if (Event.ACK_TIMEOUT.equals(event) || Event.RESPONSE_TIMEOUT.equals(event)) {
            if (ConnectionState.AWAITING_AUTHENTICATION.equals(getConnectionState())) {
                String errMsg = "Your message timed out because you're not authenticated. Have you called login()?";
                onError(Topic.ERROR, Event.NOT_AUTHENTICATED, errMsg);
                return;
            }
        }

        if (deepstreamRuntimeErrorHandler != null) {
            deepstreamRuntimeErrorHandler.onException(topic, event, msg);
        } else {
            throw new DeepstreamException(topic, event, msg);
        }

    }
}
