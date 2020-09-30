package io.deepstream;

public class MockConnection implements Connection {
    public String lastSentMessage = null;
    public ConnectionState state;

    public MockConnection() {
        this(ConnectionState.CLOSED);
    }

    public MockConnection(ConnectionState connectionState) {
        state = connectionState;
    }

    @Override
    public void sendMsg(Topic topic, Actions action, String[] data) {
        lastSentMessage = MessageBuilder.getMsg(topic, action, data);
    }

    @Override
    public void send(String message) {
        lastSentMessage = message;
    }
}
