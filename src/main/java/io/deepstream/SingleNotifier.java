package io.deepstream;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.j2objc.annotations.ObjectiveCName;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

class SingleNotifier implements ResubscribeNotifier.ResubscribeListener, TimeoutListener {

    private final Topic topic;
    private final Actions action;
    private final int timeoutDuration;
    private final Connection connection;
    private final Map<String, ArrayList<SingleNotifierCallback>> requests;
    private final AckTimeoutRegistry ackTimeoutRegistry;
    private final ResubscribeNotifier resubscribeNotifier;

    /**
     * Provides a scaffold for subscriptionless requests to io.deepstream.gherkin, such as the SNAPSHOT
     * and HAS functionality. The SingleNotifier multiplexes all the client requests so
     * that they can can be notified at once, and also includes reconnection functionality
     * incase the connection drops.
     *
     * @param client          The deepstream client
     * @param connection      The deepstream connection
     * @param topic           The Topic
     * @param action          The Action
     * @param timeoutDuration The timeout duration before an ack timeout is triggered
     */
    @ObjectiveCName("init:connection:topic:action:timeoutDuration:")
    public SingleNotifier(AbstractDeepstreamClient client, Connection connection, Topic topic, Actions action, int timeoutDuration) {
        this.ackTimeoutRegistry = client.getAckTimeoutRegistry();
        this.connection = connection;
        this.topic = topic;
        this.action = action;
        this.timeoutDuration = timeoutDuration;

        this.resubscribeNotifier = new ResubscribeNotifier(client, this);
        this.requests = new ConcurrentHashMap<>();
    }

    /**
     * Check if there is a request pending with a specified name
     *
     * @param name The name of the object being requested
     * @return true if the request exists
     */
    @ObjectiveCName("hasRequest:")
    public boolean hasRequest(String name) {
        return requests.containsKey(name);
    }

    /**
     * Add a request. If one has already been made it will skip the server request
     * and multiplex the response
     *
     * @param name                   The name of the object being requested
     * @param singleNotifierCallback The callback to call once the request is completed
     */
    @ObjectiveCName("request:utilSingleNotifierCallback:")
    public void request(String name, SingleNotifierCallback singleNotifierCallback) {
        ArrayList<SingleNotifierCallback> callbacks;
        synchronized (this) {
            callbacks = requests.get(name);
            if (callbacks == null) {
                callbacks = new ArrayList<>();
                requests.put(name, callbacks);
                send(name);
            }
        }

        callbacks.add(singleNotifierCallback);
        ackTimeoutRegistry.add(topic, action, name, Event.RESPONSE_TIMEOUT, this, timeoutDuration);
    }

    /**
     * Add a request where a response may contain more than one bit of data. Commonly used with
     * {@link SingleNotifier#receive(JsonArray, DeepstreamError)}
     *
     * @param name                   The name or version to store callbacks on
     * @param data                   The data to send in the request
     * @param action                 The action to send with the request
     * @param singleNotifierCallback The callback to call once the request is completed
     */
    public void request(String name, Actions action, String[] data, SingleNotifierCallback singleNotifierCallback) {
        ArrayList<SingleNotifierCallback> callbacks = requests.get(name);
        if (callbacks == null) {
            synchronized (this) {
                callbacks = new ArrayList<>();
                requests.put(name, callbacks);
                send(action, data);
            }
        }

        callbacks.add(singleNotifierCallback);
    }

    /**
     * Process a response for a request. This has quite a flexible API since callback functions
     * differ greatly and helps maximise reuse.
     *
     * @param name  The name of the object being requested
     * @param error An error that may have occurred during the request
     * @param data  The result data from the request
     */
    @ObjectiveCName("receive:error:data:")
    public void receive(String name, DeepstreamError error, Object data) {
        ArrayList<SingleNotifierCallback> callbacks = requests.get(name);
        for (SingleNotifierCallback callback : callbacks) {
            ackTimeoutRegistry.clear(topic, action, name);
            if (error != null) {
                callback.onSingleNotifierError(name, error);
            } else {
                callback.onSingleNotifierResponse(name, data);
            }
        }
        requests.remove(name);
    }

    /**
     * Process a response for a request. This overload of the method is for cases where
     * data from multiple messages has been merged into one deepstream message to save network
     * traffic.
     * <p>
     * Used in conjunction with {@link SingleNotifier#request(String, Actions, String[], SingleNotifierCallback)}
     *
     * @param data  The data received in the message
     * @param error Any errors from the message
     */
    public void receive(JsonArray data, DeepstreamError error) {
        for (JsonElement version : data) {
            ArrayList<SingleNotifierCallback> callbacks = requests.get(version.getAsString());
            SingleNotifierCallback cb = callbacks.get(0);
            if (error != null) {
                cb.onSingleNotifierError(null, error);
            } else {
                cb.onSingleNotifierResponse(null, null);
            }
            requests.remove(version.getAsString());
        }
    }

    void destroy() {
        this.resubscribeNotifier.destroy();
        this.requests.clear();
    }

    @Override
    public void resubscribe() {
        for (Object name : requests.keySet()) {
            send((String) name);
        }
    }

    @ObjectiveCName("send:")
    private void send(String name) {
        connection.send(MessageBuilder.getMsg(topic, action, name));
    }

    private void send(Actions action, String[] data) {
        connection.send(MessageBuilder.getMsg(topic, action, data));
    }

    @Override
    @ObjectiveCName("onTimeout:action:event:name:")
    public void onTimeout(Topic topic, Actions action, Event event, String name) {
        this.receive(name, new DeepstreamError(String.format("Response for %s timed out", name)), null);
    }

    interface SingleNotifierCallback {
        @ObjectiveCName("onSingleNotifierError:error:")
        void onSingleNotifierError(String name, DeepstreamError error);

        @ObjectiveCName("onSingleNotifierResponse:data:")
        void onSingleNotifierResponse(String name, Object data);
    }

    static class CommonRequestCallback<D> implements SingleNotifierCallback {
        private D data;
        private DeepstreamError error;

        private final CountDownLatch snapshotLatch = new CountDownLatch(1);

        @Override
        @ObjectiveCName("onSingleNotifierError:error:")
        public void onSingleNotifierError(String name, DeepstreamError error) {
            this.error = error;
            snapshotLatch.countDown();
        }

        @Override
        @ObjectiveCName("onSingleNotifierResponse:recordData:")
        public void onSingleNotifierResponse(String name, Object recordData) {
            data = (D) recordData;
            snapshotLatch.countDown();
        }

        void waitForResponse() {
            try {
                snapshotLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        D data() {
            return data;
        }

        DeepstreamError error() {
            return error;
        }

        void throwIfError() throws DeepstreamError {
            if (error != null) {
                throw error;
            }
        }
    }
}

