package io.deepstream;

import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.HashMap;

public class MockDeepstreamClient extends AbstractDeepstreamClient {
    
    private final ArrayList<ConnectionStateListener> connectionListeners = new ArrayList<>();
    private ConnectionState connectionState;

    public MockDeepstreamClient() {
        addConnectionChangeListener(getAckTimeoutRegistry());
    }

    public MockDeepstreamClient addConnectionChangeListener(ConnectionStateListener connectionStateListener) {
        connectionListeners.add(connectionStateListener);
        return this;
    }

    public MockDeepstreamClient removeConnectionChangeListener(ConnectionStateListener connectionStateListener) {
        return null;
    }

    public ConnectionState getConnectionState() {
        return connectionState;
    }

    public void setConnectionState(ConnectionState state) {
        this.connectionState = state;
        for (ConnectionStateListener listener : connectionListeners) {
            listener.connectionStateChanged(state);
        }
    }

    @Override
    LoginResult login() {
        return new LoginResult(true, new HashMap<>());
    }

    @Override
    LoginResult login(JsonElement data) {
        return new LoginResult(true, new HashMap<>());
    }

    @Override
    AbstractDeepstreamClient close() {
        return this;
    }

    public String getUid() {
        return "1";
    }
}
