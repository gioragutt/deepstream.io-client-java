package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

/**
 * This object provides a number of methods that allow a rpc provider
 * to respond to a request
 */
public class RpcResponse {

    private final IConnection connection;
    private final String name;
    private final String correlationId;

    private boolean isAcknowledged = false;
    private boolean isComplete = false;

    /**
     * This object provides a number of methods that allow a rpc provider
     * to respond to a request
     *
     * @param connection    the clients connection object
     * @param name          the name of the rpc
     * @param correlationId the correlationId for the RPC
     */
    @ObjectiveCName("init:name:correlationId:")
    RpcResponse(IConnection connection, String name, String correlationId) {
        this.connection = connection;
        this.name = name;
        this.correlationId = correlationId;
        ack();
    }

    /**
     * Acknowledges the receipt of the request. This
     * will happen implicitly unless the request callback
     * explicitly sets autoAck to false
     */
    public void ack() {
        if (isAcknowledged) {
            return;
        }

        String[] payload = {Actions.REQUEST.toString(), name, correlationId};
        connection.sendMsg(Topic.RPC, Actions.ACK, payload);
        isAcknowledged = true;
    }

    /**
     * Reject the request. This might be necessary if the client
     * is already processing a large number of requests. If deepstream
     * receives a rejection message it will try to route the request to
     * another provider - or return a NO_RPC_PROVIDER error if there are no
     * providers left
     */
    public void reject() {
        isComplete = true;
        isAcknowledged = true;
        connection.sendMsg(Topic.RPC, Actions.REJECTION, new String[]{name, correlationId});
    }

    /**
     * Completes the request by sending the response data
     * to the server. If data is an array or object it will
     * automatically be serialised.<br/>
     * If autoAck is disabled and the response is sent before
     * the ack message the request will still be completed and the
     * ack message ignored
     *
     * @param data the data send by the provider. Has to be JsonSerializable
     */
    @ObjectiveCName("send:")
    public void send(Object data) {
        if (isComplete) {
            throw new DeepstreamException(String.format("Rpc %s already completed", name));
        }

        String typedData = MessageBuilder.typed(data);
        String[] payload = {name, correlationId, typedData};
        connection.sendMsg(Topic.RPC, Actions.RESPONSE, payload);
        isComplete = true;
    }

    /**
     * Notifies the server that an error has occurred while trying to process the request.
     * This will complete the rpc.
     *
     * @param errorMsg the message used to describe the error that occurred
     */
    @ObjectiveCName("error:")
    public void error(String errorMsg) {
        isComplete = true;
        isAcknowledged = true;
        String[] payload = {errorMsg, name, correlationId};
        connection.sendMsg(Topic.RPC, Actions.ERROR, payload);
    }
}
