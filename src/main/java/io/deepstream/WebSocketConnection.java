package io.deepstream;

import com.google.gson.JsonElement;
import com.google.j2objc.annotations.ObjectiveCName;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Establishes a connection to a deepstream server, either
 * using TCP or via a WebSocket
 */
class WebSocketConnection implements Connection {

    private final DeepstreamClient client;
    private final String originalUrl;
    private final java.util.List<ConnectionStateListener> connectStateListeners = new ArrayList<>();
    private final DeepstreamConfig options;
    private final EndpointFactory endpointFactory;
    private final ExecutorService rpcThread = Executors.newSingleThreadExecutor();
    private final ExecutorService recordThread = Executors.newSingleThreadExecutor();
    private final ExecutorService eventThread = Executors.newSingleThreadExecutor();
    private final ExecutorService presenceThread = Executors.newSingleThreadExecutor();
    private Endpoint endpoint;
    private boolean tooManyAuthAttempts = false;
    private boolean challengeDenied = false;
    private boolean deliberateClose = false;
    private boolean redirecting = false;
    private Timer reconnectTimeout = null;
    private int reconnectionAttempt = 0;
    private StringBuilder messageBuffer = new StringBuilder();
    private String url;
    private ConnectionState connectionState = ConnectionState.CLOSED;
    private GlobalConnectivityState globalConnectivityState = GlobalConnectivityState.DISCONNECTED;
    private DeepstreamClient.LoginCallback loginCallback;
    private JsonElement authParameters;

    /**
     * Creates an endpoint and passed it to {@link WebSocketConnection#WebSocketConnection(String, DeepstreamConfig, DeepstreamClient, EndpointFactory, Endpoint)}
     *
     * @param url             The endpoint url
     * @param options         The options used to initialise the deepstream client
     * @param endpointFactory The factory to create endpoints
     * @param client          The deepstream client
     * @throws URISyntaxException An exception if an invalid url is passed in
     * @see WebSocketConnection#WebSocketConnection(String, DeepstreamConfig, DeepstreamClient, EndpointFactory, Endpoint)
     */
    WebSocketConnection(String url,
                        DeepstreamConfig options,
                        DeepstreamClient client,
                        EndpointFactory endpointFactory) throws URISyntaxException {
        this(url, options, client, endpointFactory, null);
        endpoint = createEndpoint();
    }

    /**
     * Creates a connection, that is responsible for handling all the connection related logic related to state
     * and messages
     *
     * @param url      The endpoint url* @param options The options used to initialise the deepstream client
     * @param client   The deepstream client
     * @param endpoint The endpoint, whether WebSocket, mock or anything else
     */
    WebSocketConnection(String url,
                        DeepstreamConfig options,
                        DeepstreamClient client,
                        Endpoint endpoint) {
        this(url, options, client, null, endpoint);
    }

    public WebSocketConnection(String url,
                               DeepstreamConfig options,
                               DeepstreamClient client,
                               EndpointFactory endpointFactory,
                               Endpoint endpoint) {
        this.client = client;
        this.originalUrl = url;
        this.url = url;
        this.options = options;
        this.endpoint = endpoint;
        this.endpointFactory = endpointFactory;
    }

    /**
     * Authenticate the user connection
     *
     * @param authParameters The authentication parameters to send to deepstream
     * @param loginCallback  The callback for a successful / unsuccessful login attempt
     *                       connection
     */
    @ObjectiveCName("authenticate:loginCallback:")
    void authenticate(JsonElement authParameters, DeepstreamClient.LoginCallback loginCallback) {
        this.loginCallback = loginCallback;
        this.authParameters = Utils.ensureValidJsonObject(authParameters);

        if (tooManyAuthAttempts || challengeDenied) {
            client.onError(Topic.ERROR, Event.IS_CLOSED, "The client's connection was closed");
            loginCallback.loginFailed(Event.IS_CLOSED, "The client's connection was closed");
            return;
        }

        if (connectionState == ConnectionState.AWAITING_AUTHENTICATION) {
            sendAuthMessage();
        }
    }

    @Override
    @ObjectiveCName("send:")
    public void send(String message) {
        if (connectionState != ConnectionState.OPEN) {
            messageBuffer.append(message);
        } else {
            endpoint.send(message);
        }
    }

    @Override
    @ObjectiveCName("sendMsg:action:data:")
    public void sendMsg(Topic topic, Actions action, String[] data) {
        send(MessageBuilder.getMsg(topic, action, data));
    }

    private void sendAuthMessage() {
        setState(ConnectionState.AUTHENTICATING);
        String authMessage = MessageBuilder.getMsg(
                Topic.AUTH,
                Actions.REQUEST,
                Utils.ensureValidJsonObject(authParameters).toString());
        endpoint.send(authMessage);
    }

    void addConnectionChangeListener(ConnectionStateListener connectionStateListener) {
        connectStateListeners.add(connectionStateListener);
    }

    void removeConnectionChangeListener(ConnectionStateListener connectionStateListener) {
        connectStateListeners.remove(connectionStateListener);
    }

    ConnectionState getConnectionState() {
        return connectionState;
    }

    public void close(boolean forceClose) {
        deliberateClose = true;

        if (forceClose && endpoint != null) {
            endpoint.forceClose();
        } else if (endpoint != null) {
            endpoint.close();
            endpoint = null;
        }
        if (reconnectTimeout != null) {
            reconnectTimeout.cancel();
            reconnectTimeout = null;
        }

        recordThread.shutdown();
        eventThread.shutdown();
        rpcThread.shutdown();
        presenceThread.shutdown();
    }

    void onOpen() {
        setState(ConnectionState.AWAITING_CONNECTION);
    }

    @ObjectiveCName("onError:")
    void onError(final String error) {
        setState(ConnectionState.ERROR);

        /*
         * If the implementation isn't listening on the error event this will throw
         * an error. So let's defer it to allow the reconnection to kick in.
         */
        new Timer().schedule(new TimerTask() {
            public void run() {
                client.onError(null, Event.CONNECTION_ERROR, error);
            }
        }, 1000);
    }

    @ObjectiveCName("onMessage:")
    void onMessage(String rawMessage) {
        List<Message> parsedMessages = MessageParser.parse(rawMessage, client);
        for (final Message message : parsedMessages) {
            switch (message.topic) {
                case CONNECTION:
                    handleConnectionResponse(message);
                    break;
                case AUTH:
                    handleAuthResponse(message);
                    break;
                case EVENT:
                    eventThread.execute(() -> client.event.handle(message));
                    break;
                case RPC:
                    rpcThread.execute(() -> client.rpc.handle(message));
                    break;
                case RECORD:
                    recordThread.execute(() -> client.record.handle(message));
                    break;
                case PRESENCE:
                    presenceThread.execute(() -> client.presence.handle(message));
                    break;
                default:
                    client.onError(Topic.ERROR, Event.UNSOLICITED_MESSAGE, message.action.toString());
                    break;
            }
        }
    }

    void onClose() throws URISyntaxException {
        if (redirecting) {
            redirecting = false;
            endpoint = createEndpoint();
            return;
        }

        if (deliberateClose) {
            setState(ConnectionState.CLOSED);
            return;
        }

        setState(ConnectionState.ERROR);
        if (originalUrl.equals(url)) {
            tryReconnect();
            return;
        }

        url = originalUrl;
        endpoint = createEndpoint();
    }

    @ObjectiveCName("handleConnectionResponse:")
    private void handleConnectionResponse(Message message) {
        switch (message.action) {
            case PING:
                endpoint.send(MessageBuilder.getMsg(Topic.CONNECTION, Actions.PONG));
                break;
            case ACK:
                setState(ConnectionState.AWAITING_AUTHENTICATION);
                break;
            case CHALLENGE:
                setState(ConnectionState.CHALLENGING);
                endpoint.send(MessageBuilder.getMsg(
                        Topic.CONNECTION,
                        Actions.CHALLENGE_RESPONSE,
                        originalUrl));
                break;
            case REJECTION:
                challengeDenied = true;
                close(false);
                break;
            case REDIRECT:
                url = message.data[0];
                redirecting = true;
                endpoint.close();
                endpoint = null;
                break;
        }
    }

    @ObjectiveCName("handleAuthResponse:")
    private void handleAuthResponse(Message message) {
        if (message.action == Actions.ERROR) {
            if (message.data[0].equals(Event.TOO_MANY_AUTH_ATTEMPTS.name())) {
                deliberateClose = true;
                tooManyAuthAttempts = true;
            } else {
                authParameters = null;
                setState(ConnectionState.AWAITING_AUTHENTICATION);
            }

            if (loginCallback != null) {
                loginCallback.loginFailed(
                        Event.getEvent(message.data[0]),
                        MessageParser.convertTyped(message.data[1], client, options.getJsonParser()));
            }
        } else if (message.action == Actions.ACK) {
            setState(ConnectionState.OPEN);

            if (messageBuffer.length() > 0) {
                endpoint.send(messageBuffer.toString());
                messageBuffer = new StringBuilder();
            }

            if (loginCallback != null) {
                try {
                    Object data = MessageParser.convertTyped(message.data[0], client, options.getJsonParser());
                    loginCallback.loginSuccess(data);
                } catch (IndexOutOfBoundsException e) {
                    loginCallback.loginSuccess(null);
                }
            }
        }
    }

    @ObjectiveCName("setState:")
    private void setState(ConnectionState connectionState) {
        this.connectionState = connectionState;

        for (ConnectionStateListener connectStateListener : connectStateListeners) {
            connectStateListener.connectionStateChanged(connectionState);
        }

        if (connectionState == ConnectionState.AWAITING_AUTHENTICATION && authParameters != null) {
            sendAuthMessage();
        }
    }

    /**
     * Set global connectivity state.
     *
     * @param globalConnectivityState Current global connectivity state
     */
    protected void setGlobalConnectivityState(GlobalConnectivityState globalConnectivityState) {
        this.globalConnectivityState = globalConnectivityState;

        if (shouldReconnectAfterGlobalConnectivityChange()) {
            tryReconnect();
            return;
        }

        if (reconnectTimeout != null) {
            reconnectTimeout.cancel();
        }
        reconnectTimeout = null;
        reconnectionAttempt = 0;
        endpoint.forceClose();
        setState(ConnectionState.CLOSED);
    }

    private boolean shouldReconnectAfterGlobalConnectivityChange() {
        return globalConnectivityState == GlobalConnectivityState.CONNECTED &&
                (connectionState == ConnectionState.CLOSED || connectionState == ConnectionState.ERROR);
    }

    /**
     * Take the url passed when creating the client and ensure the correct
     * protocol is provided
     *
     * @param url         Url passed in by client
     * @param defaultPath Default path to concatenate if one doest not exist
     * @return Url with supported protocol
     */
    private URI parseUri(String url, String defaultPath) throws URISyntaxException {
        if (url.startsWith("http:") || url.startsWith("https:")) {
            throw new URISyntaxException(url, "HTTP/HTTPS is not supported, please use ws or wss instead");
        }
        if (url.startsWith("//")) {
            url = "ws:" + url;
        } else if (!url.startsWith("ws:") && !url.startsWith("wss:")) {
            url = "ws://" + url;
        }
        URI uri = new URI(url);
        if (uri.getPath().isEmpty()) {
            uri = uri.resolve(defaultPath);
        }
        return uri;
    }

    private Endpoint createEndpoint() throws URISyntaxException {
        URI uri = parseUri(url, options.getPath());
        Endpoint endpoint = endpointFactory.createEndpoint(uri, this);
        endpoint.open();
        return endpoint;
    }

    private void tryReconnect() {
        if (reconnectTimeout != null) {
            return;
        }

        int maxReconnectAttempts = options.getMaxReconnectAttempts();
        int reconnectIntervalIncrement = options.getReconnectIntervalIncrement();
        int maxReconnectInterval = options.getMaxReconnectInterval();

        if (reconnectionAttempt >= maxReconnectAttempts) {
            clearReconnect();
            close(true);
            return;
        }

        if (globalConnectivityState == GlobalConnectivityState.CONNECTED) {
            setState(ConnectionState.RECONNECTING);
            reconnectTimeout = new Timer();

            int reconnectInterval = Math.min(
                    reconnectIntervalIncrement * reconnectionAttempt,
                    maxReconnectInterval);

            reconnectTimeout.schedule(new TimerTask() {
                public void run() {
                    tryOpen();
                }
            }, reconnectInterval);
            reconnectionAttempt++;
        }
    }

    private void tryOpen() {
        reconnectTimeout.cancel();
        reconnectTimeout = null;
        endpoint.open();
    }

    private void clearReconnect() {
        reconnectTimeout = null;
        reconnectionAttempt = 0;
    }
}
