package io.deepstream;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class RpcHandlerTest {
    DeepstreamRuntimeErrorHandler errorCallbackMock = mock(DeepstreamRuntimeErrorHandler.class);
    MockDeepstreamClient deepstreamClientMock = TestUtil.createMockDeepstreamClient(errorCallbackMock);
    MockConnection mockConnection = new MockConnection(ConnectionState.OPEN);
    RpcHandler rpcHandler;
    int rpcCalls = 0;

    RpcRequestedListener addTwoCallback = (String rpcName, Object data, RpcResponse response) -> {
        rpcCalls++;
        double numA = ((JsonElement) data).getAsJsonObject().get("numA").getAsDouble();
        double numB = ((JsonElement) data).getAsJsonObject().get("numB").getAsDouble();
        response.send(numA + numB);
    };

    @Before
    public void setUp() throws InvalidDeepstreamConfig {
        Properties options = new Properties();
        options.put("subscriptionTimeout", "10");
        options.put("rpcAckTimeout", "10");
        options.put("rpcResponseTimeout", "30");

        rpcHandler = new RpcHandler(
                new DeepstreamConfig(options),
                mockConnection,
                deepstreamClientMock);
    }

    private void handleRpc(Actions actionType, String... payload) {
        rpcHandler.handle(new Message("raw", Topic.RPC, actionType, payload));
    }

    private AtomicReference<RpcResult> makeRequestInThread(JsonObject data) {
        final AtomicReference<RpcResult> result = new AtomicReference<>();
        new Thread(() -> result.set(rpcHandler.make("addTwo", data))).start();
        return result;
    }

    @Test
    public void registersAProvider() {
        rpcHandler.provide("addTwo", addTwoCallback);

        TestUtil.assertLastMessageWas(mockConnection, "P|S|addTwo+");
        Assert.assertEquals(rpcCalls, 0);
    }

    @Test
    public void errorsIfNoAckReceivedForProvide() throws InterruptedException {
        rpcHandler.provide("addTwo", addTwoCallback);

        Thread.sleep(50);

        verify(errorCallbackMock, times(1))
                .onException(
                        Topic.RPC,
                        Event.ACK_TIMEOUT,
                        "No ACK message received in time for SUBSCRIBE addTwo");
    }

    @Test
    public void repliesToRpcRequest() {
        rpcHandler.provide("addTwo", addTwoCallback);

        handleRpc(Actions.REQUEST, "addTwo", "123", "O{\"numA\":7,\"numB\":3}");

        TestUtil.assertLastMessageWas(mockConnection, "P|RES|addTwo|123|N10.0+");
    }

    @Test
    public void sendsRejectionIfNoProviderExists() {
        handleRpc(Actions.REQUEST, "doesNotExist", "123", "O{\"numA\":7,\"numB\":3}");

        TestUtil.assertLastMessageWas(mockConnection, "P|REJ|doesNotExist|123+");
    }

    @Test
    public void unprovideAProvider() {
        rpcHandler.provide("addTwo", addTwoCallback);
        rpcHandler.unprovide("addTwo");

        TestUtil.assertLastMessageWas(mockConnection, "P|US|addTwo+");
    }

    @Test
    public void doesNotCallUnregisteredProvider() {
        rpcHandler.provide("addTwo", addTwoCallback);
        rpcHandler.unprovide("addTwo");

        handleRpc(Actions.REQUEST, "doesNotExist", "123", "O{\"numA\":7,\"numB\":3}");

        TestUtil.assertLastMessageWas(mockConnection, "P|REJ|doesNotExist|123+");
    }

    @Test
    public void makesSuccessfulRpcFor_addTwo() throws InterruptedException {
        final JsonObject data = new JsonObject();
        data.addProperty("numA", 3);
        data.addProperty("numB", 8);

        AtomicReference<RpcResult> rpcResponse = makeRequestInThread(data);

        Thread.sleep(20);
        handleRpc(Actions.RESPONSE, "addTwo", "1", "N11");
        Thread.sleep(20);

        TestUtil.assertLastMessageWas(mockConnection, "P|REQ|addTwo|1|O{\"numA\":3,\"numB\":8}+");
        Assert.assertTrue(rpcResponse.get().success());
        Assert.assertEquals(rpcResponse.get().getData(), (float) 11.0);
    }

    @Test
    public void makesRpcFor_addTwoButReceivesError() throws InterruptedException {
        final JsonObject data = new JsonObject();
        data.addProperty("numA", 3);
        data.addProperty("numB", 8);

        final AtomicReference<RpcResult> rpcResponse = makeRequestInThread(data);

        Thread.sleep(20);
        TestUtil.assertLastMessageWas(mockConnection, "P|REQ|addTwo|1|O{\"numA\":3,\"numB\":8}+");

        handleRpc(Actions.ERROR, "NO_PROVIDER", "addTwo", "1");

        Thread.sleep(20);

        TestUtil.assertLastMessageWas(mockConnection, "P|REQ|addTwo|1|O{\"numA\":3,\"numB\":8}+");
        Assert.assertFalse(rpcResponse.get().success());
        Assert.assertEquals(rpcResponse.get().getData(), "NO_PROVIDER");
    }

    @Test
    public void makesRpcFor_addTwoButDoesNotReceiveAck() throws InterruptedException {
        final JsonObject data = new JsonObject();
        data.addProperty("numA", 3);
        data.addProperty("numB", 8);

        new Thread(() -> rpcHandler.make("addTwo", data)).start();

        Thread.sleep(200);
        TestUtil.assertLastMessageWas(mockConnection, "P|REQ|addTwo|1|O{\"numA\":3,\"numB\":8}+");
        Thread.sleep(250);

        verify(errorCallbackMock, times(1))
                .onException(
                        Topic.RPC,
                        Event.ACK_TIMEOUT,
                        "No ACK message received in time for REQUEST 1");
    }

    @Test
    public void makesRpcFor_addTwoButDoesNotReceiveResponse() throws InterruptedException {
        final JsonObject data = new JsonObject();
        data.addProperty("numA", 3);
        data.addProperty("numB", 8);

        final AtomicReference<RpcResult> rpcResponse = makeRequestInThread(data);

        Thread.sleep(20);
        TestUtil.assertLastMessageWas(mockConnection, "P|REQ|addTwo|1|O{\"numA\":3,\"numB\":8}+");

        Thread.sleep(100);
        Assert.assertFalse(rpcResponse.get().success());
        Assert.assertEquals(rpcResponse.get().getData(), Event.RESPONSE_TIMEOUT.toString());
    }
}
