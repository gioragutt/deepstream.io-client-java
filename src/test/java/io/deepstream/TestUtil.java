package io.deepstream;

import org.junit.Assert;

class TestUtil {

    private static final char MPS = '\u001f';
    private static final char MS = '\u001e';

    static String formatMessage(String input) {
        return input
                .replaceAll("<UID>", "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}")
                .replace('|', MPS)
                .replace('+', MS);
    }

    static MockDeepstreamClient createMockDeepstreamClient() {
        return createMockDeepstreamClient((topic, event, errorMessage) -> {
        });
    }

    static MockDeepstreamClient createMockDeepstreamClient(
            DeepstreamRuntimeErrorHandler deepstreamRuntimeErrorHandler) {
        MockDeepstreamClient mockDeepstreamClient = new MockDeepstreamClient();
        mockDeepstreamClient.setConnectionState(ConnectionState.OPEN);
        mockDeepstreamClient.setRuntimeErrorHandler(deepstreamRuntimeErrorHandler);
        return mockDeepstreamClient;
    }

    static void delay(long millis, Runnable action) {
        new Thread(() -> {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            action.run();
        }).start();
    }

    static void assertLastMessageWas(MockConnection mockConnection, String s) {
        Assert.assertEquals(
                formatMessage(s),
                mockConnection.lastSentMessage);
    }

    static void assertLastMessageWas(MockEndpoint mockEndpoint, String s) {
        Assert.assertEquals(
                s,
                mockEndpoint.lastSentMessage);
    }
}
