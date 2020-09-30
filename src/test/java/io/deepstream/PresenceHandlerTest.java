package io.deepstream;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.mockito.Mockito.*;

public class PresenceHandlerTest {

    MockDeepstreamClient deepstreamClientMock = TestUtil.createMockDeepstreamClient();
    MockConnection mockConnection = new MockConnection();
    PresenceHandler presenceHandler;
    PresenceEventListener presenceCallback = mock(PresenceEventListener.class);

    @Before
    public void setUp() throws InvalidDeepstreamConfig {
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

        TestUtil.assertLastMessageWas(mockConnection, "U|Q|Q+");
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

        TestUtil.assertLastMessageWas(mockConnection, "U|Q|Q+");
        String[] expectedClients = new String[]{"Bart", "Homer"};
        Assert.assertArrayEquals(expectedClients, clients);
    }

    @Test
    public void subscribesToLoginsAndIsAlertedWhenClientLogsIn() {
        presenceHandler.subscribe(presenceCallback);
        TestUtil.assertLastMessageWas(mockConnection, "U|S|S+");

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
        TestUtil.assertLastMessageWas(mockConnection, "U|US|US+");

        presenceHandler.handle(new Message(
                "raw",
                Topic.PRESENCE,
                Actions.PRESENCE_JOIN,
                new String[]{"Homer"}));

        verify(presenceCallback, times(0)).onClientLogin("Homer");
    }
}
