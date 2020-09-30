package io.deepstream;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class ResubscribeNotifierTest {

    private final MockDeepstreamClient deepstreamClientMock =
            TestUtil.createMockDeepstreamClient();

    private final ResubscribeNotifier.ResubscribeListener resubscribeCallbackMock =
            mock(ResubscribeNotifier.ResubscribeListener.class);

    private final ResubscribeNotifier resubscribeNotifier =
            new ResubscribeNotifier(deepstreamClientMock, resubscribeCallbackMock);

    @After
    public void tearDown() {
        resubscribeNotifier.destroy();
    }

    @Test
    public void resubscribeCallbackNotCalledWhenReconnecting() {
        deepstreamClientMock.setConnectionState(ConnectionState.RECONNECTING);
        verify(resubscribeCallbackMock, times(0)).resubscribe();
    }

    @Test
    public void resubscribeCallbackNotCalledWhenOpening() {
        deepstreamClientMock.setConnectionState(ConnectionState.OPEN);
        verify(resubscribeCallbackMock, times(0)).resubscribe();
    }

    @Test
    public void resubscribeCallbackCalledWhenReconnectingAndOpen() {
        deepstreamClientMock.setConnectionState(ConnectionState.RECONNECTING);
        verify(resubscribeCallbackMock, times(0)).resubscribe();

        deepstreamClientMock.setConnectionState(ConnectionState.OPEN);
        verify(resubscribeCallbackMock, times(1)).resubscribe();
    }


}