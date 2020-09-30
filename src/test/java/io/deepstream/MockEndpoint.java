package io.deepstream;

import java.util.ArrayList;

public class MockEndpoint implements Endpoint {
    public String url;
    public String lastSentMessage;
    public ArrayList<String> sentMessages = new ArrayList<>();
    private WebSocketConnection connection;

    public MockEndpoint(String url, WebSocketConnection connection) {
        this.connection = connection;
        this.url = url;
    }

    public void setConnection(WebSocketConnection connection) {
        this.connection = connection;
    }

    public void sendOpenEvent() {
        connection.onOpen();
    }

    public void sendMessage(String message) {
        try {
            connection.onMessage(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendError(Exception exception) {
        connection.onError(exception.getMessage());
    }

    public void send(String message) {
        this.lastSentMessage = message;
        this.sentMessages.add(message);
    }

    @Override
    public void close() {

    }

    @Override
    public void open() {

    }

    @Override
    public void forceClose() {

    }
}
