package io.deepstream;

public class MockConnection implements IConnection {

    public String lastSentMessage;
    public ConnectionState state;

    public MockConnection() {
        this.lastSentMessage = null;
        this.state = ConnectionState.CLOSED;
    }

    @Override
    public void sendMsg(Topic topic, Actions action, String[] data) {
        this.lastSentMessage = MessageBuilder.getMsg(topic, action, data);
    }

    @Override
    public void send(String message) {
        this.lastSentMessage = message;
    }
}
