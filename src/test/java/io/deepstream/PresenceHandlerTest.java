package io.deepstream;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.mockito.Mockito.*;

public class PresenceHandlerTest {

    MockDeepstreamClient deepstreamClientMock;
    MockConnection mockConnection;
    PresenceHandler presenceHandler;
    PresenceEventListener presenceCallback;
    DeepstreamRuntimeErrorHandler deepstreamRuntimeErrorHandler;

    @Before
    public void setUp() throws InvalidDeepstreamConfig {
        presenceCallback = mock(PresenceEventListener.class);
        mockConnection = new MockConnection();
        deepstreamRuntimeErrorHandler = mock(DeepstreamRuntimeErrorHandler.class);
        deepstreamClientMock = new MockDeepstreamClient();
        deepstreamClientMock.setRuntimeErrorHandler(this.deepstreamRuntimeErrorHandler);
        deepstreamClientMock.setConnectionState(ConnectionState.OPEN);

        Properties options = new Properties();
        options.put("subscriptionTimeout", "1000");

        presenceHandler = new PresenceHandler(
                new DeepstreamConfig(options),
                mockConnection,
                deepstreamClientMock);
    }

    @Test
    public void queriesForConnectedClientsAndReceiveEmptyArray() throws DeepstreamError {
        Assert.assertNull(mockConnection.lastSentMessage);

        TestUtil.delay(100, () -> presenceHandler.handle(new Message(
                "raw",
                Topic.PRESENCE,
                Actions.QUERY,
                new String[]{})));

        String[] clients = presenceHandler.getAll();

        Assert.assertEquals(TestUtil.formatMessage("U|Q|Q+"), mockConnection.lastSentMessage);
        Assert.assertArrayEquals(new String[]{}, clients);
    }

    @Test
    public void queriesForClientsAndReceivesClientArray() throws DeepstreamError {
        TestUtil.delay(100, () -> presenceHandler.handle(new Message(
                "raw",
                Topic.PRESENCE,
                Actions.QUERY,
                new String[]{"Bart", "Homer"})));

        String[] clients = presenceHandler.getAll();

        Assert.assertEquals(TestUtil.formatMessage("U|Q|Q+"), mockConnection.lastSentMessage);
        String[] expectedClients = new String[]{"Bart", "Homer"};
        Assert.assertArrayEquals(expectedClients, clients);
    }

    @Test
    public void subscribesToLoginsAndIsAlertedWhenClientLogsIn() {
        presenceHandler.subscribe(presenceCallback);
        Assert.assertEquals(TestUtil.formatMessage("U|S|S+"), mockConnection.lastSentMessage);

        presenceHandler.handle(new Message(
                "raw",
                Topic.PRESENCE,
                Actions.PRESENCE_JOIN,
                new String[]{"Homer"}));

        verify(presenceCallback, times(1)).onClientLogin("Homer");
    }

    @Test
    public void unsubscribesToLoginsAndIsNotAlertedWhenClientLogsIn() {
        presenceHandler.unsubscribe(presenceCallback);
        Assert.assertEquals(TestUtil.formatMessage("U|US|US+"), mockConnection.lastSentMessage);

        presenceHandler.handle(new Message(
                "raw",
                Topic.PRESENCE,
                Actions.PRESENCE_JOIN,
                new String[]{"Homer"}));

        verify(presenceCallback, times(0)).onClientLogin("Homer");
    }
}
