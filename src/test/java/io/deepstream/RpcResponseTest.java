package io.deepstream;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Properties;

import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class RpcResponseTest {

    DeepstreamClient deepstreamClientMock = mock(DeepstreamClient.class);
    MockConnection mockConnection = new MockConnection();
    RpcHandler rpcHandler;

    @Before
    public void setUp() throws InvalidDeepstreamConfig {
        Properties options = new Properties();
        options.put("subscriptionTimeout", "2000");
        options.put("rpcAckTimeout", "6000");
        options.put("rpcResponseTimeout", "10000");

        rpcHandler = new RpcHandler(new DeepstreamConfig(options), mockConnection, deepstreamClientMock);
    }

    @Test
    public void sendsAckMessageAutomatically() {
        new RpcResponse(mockConnection, "addTwo", "123");
        TestUtil.assertLastMessageWas(mockConnection, "P|A|REQ|addTwo|123+");
    }

    @Test
    public void sendsTheResponse() {
        RpcResponse response = new RpcResponse(mockConnection, "addTwo", "123");
        response.send(14);
        TestUtil.assertLastMessageWas(mockConnection, "P|RES|addTwo|123|N14+");
    }

    @Test
    public void rejectsTheMessage() {
        RpcResponse response = new RpcResponse(mockConnection, "addTwo", "123");
        response.reject();
        TestUtil.assertLastMessageWas(mockConnection, "P|REJ|addTwo|123+");
    }

    @Test
    public void throwsWhenSendingRejectedMessage() {
        RpcResponse response = new RpcResponse(mockConnection, "addTwo", "123");
        response.reject();
        try {
            response.send("bla");
        } catch (DeepstreamException ex) {
            Assert.assertTrue(ex.getMessage().contains("Rpc addTwo already completed"));
        }
    }

    @Test
    public void errorsTheMessage() {
        RpcResponse response = new RpcResponse(mockConnection, "addTwo", "123");
        response.error("Error Message");
        TestUtil.assertLastMessageWas(mockConnection, "P|E|Error Message|addTwo|123+");
    }

    @Test
    public void throwsWhenSendingErroredMessage() {
        RpcResponse response = new RpcResponse(mockConnection, "addTwo", "123");
        response.error("Err msg");
        try {
            response.send("bla");
        } catch (DeepstreamException ex) {
            Assert.assertTrue(ex.getMessage().contains("Rpc addTwo already completed"));
        }
    }
}