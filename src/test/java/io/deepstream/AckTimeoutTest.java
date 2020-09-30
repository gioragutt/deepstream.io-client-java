package io.deepstream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class AckTimeoutTest {
    private static final String NO_ACE_RECEIVED_MESSAGE =
            "No ACK message received in time for SUBSCRIBE Event1";

    private final DeepstreamRuntimeErrorHandler errorHandler =
            mock(DeepstreamRuntimeErrorHandler.class);

    private final MockDeepstreamClient deepstreamClientMock =
            TestUtil.createMockDeepstreamClient(errorHandler);

    private final AckTimeoutRegistry ackTimeoutRegistry =
            deepstreamClientMock.getAckTimeoutRegistry();

    @Test
    public void onTimeoutCalledWhenNoAckReceived() throws InterruptedException {
        ackTimeoutRegistry.add(Topic.EVENT, Actions.SUBSCRIBE, "Event1", 20);

        Thread.sleep(50);
        verify(errorHandler, times(1))
                .onException(Topic.EVENT, Event.ACK_TIMEOUT, NO_ACE_RECEIVED_MESSAGE);
    }

    @Test
    public void onTimeoutNotCalledWhenAckReceived() throws InterruptedException {
        String[] subscribePayload = {Actions.SUBSCRIBE.toString(), "Event1"};
        Message message = new Message(null, Topic.EVENT, Actions.ACK, subscribePayload);

        ackTimeoutRegistry.add(Topic.EVENT, Actions.SUBSCRIBE, "Event1", 20);
        ackTimeoutRegistry.clear(message);

        Thread.sleep(50);
        verify(errorHandler, times(0))
                .onException(Topic.EVENT, Event.ACK_TIMEOUT, NO_ACE_RECEIVED_MESSAGE);
    }

    @Test
    public void acksNotSentUntilConnectionStateIsOpen() throws InterruptedException {
        deepstreamClientMock.setConnectionState(ConnectionState.CLOSED);
        ackTimeoutRegistry.add(Topic.EVENT, Actions.SUBSCRIBE, "Event1", 20);

        Thread.sleep(50);
        verify(errorHandler, times(0))
                .onException(Topic.EVENT, Event.ACK_TIMEOUT, NO_ACE_RECEIVED_MESSAGE);

        deepstreamClientMock.setConnectionState(ConnectionState.OPEN);
        Thread.sleep(50);
        verify(errorHandler, times(1))
                .onException(Topic.EVENT, Event.ACK_TIMEOUT, NO_ACE_RECEIVED_MESSAGE);
    }

    @Test
    public void acksAddedTwiceOnlySentOnce() throws InterruptedException {
        ackTimeoutRegistry.add(Topic.EVENT, Actions.SUBSCRIBE, "Event1", 20);
        ackTimeoutRegistry.add(Topic.EVENT, Actions.SUBSCRIBE, "Event1", 20);

        Thread.sleep(50);
        verify(errorHandler, times(1))
                .onException(Topic.EVENT, Event.ACK_TIMEOUT, NO_ACE_RECEIVED_MESSAGE);
    }
}