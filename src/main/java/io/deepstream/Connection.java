package io.deepstream;

/**
 * TODO: Remove somehow?
 */
interface Connection {
    void sendMsg(Topic topic, Actions action, String[] data);

    void send(String message);
}
