package io.deepstream;

import com.google.gson.JsonObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class WebSocketConnectionTest {

    private static final String ORIGINAL_URL = "originalProtocol://originalHost:originalPort";

    private final DeepstreamClient deepstreamClientMock =
            mock(DeepstreamClient.class);
    private final ConnectionStateListener connectionStateListenerMock =
            mock(ConnectionStateListener.class);
    private final DeepstreamClient.LoginCallback loginCallback =
            mock(DeepstreamClient.LoginCallback.class);

    private final MockEndpoint mockEndpoint =
            new MockEndpoint(ORIGINAL_URL, null);

    private final WebSocketConnection connection = new WebSocketConnection(
            ORIGINAL_URL,
            new DeepstreamConfig(),
            deepstreamClientMock,
            mockEndpoint);

    @Before
    public void setUp() {
        mockEndpoint.setConnection(connection);
        connection.addConnectionChangeListener(connectionStateListenerMock);
    }

    @Test
    public void initialState() {
        mockEndpoint.sendOpenEvent();
        verifyConnectionState(ConnectionState.AWAITING_CONNECTION);
    }

    @Test
    public void challengeReceivedSendsOriginalUrl() {
        mockEndpoint.sendMessage(MessageBuilder.getMsg(Topic.CONNECTION, Actions.CHALLENGE));
        verifyConnectionState(ConnectionState.CHALLENGING);
        TestUtil.assertLastMessageWas(
                mockEndpoint,
                MessageBuilder.getMsg(Topic.CONNECTION, Actions.CHALLENGE_RESPONSE, ORIGINAL_URL));
    }

    //TODO: Challenge response redirect
    @Test
    public void challengeRedirect() {
    }

    @Test
    public void challengeAck() {
        mockEndpoint.sendMessage(MessageBuilder.getMsg(Topic.CONNECTION, Actions.ACK));
        verifyConnectionState(ConnectionState.AWAITING_AUTHENTICATION);
    }

    @Test
    public void sendingAuthentication() {
        challengeAck();
        JsonObject authParams = new JsonObject();
        authParams.addProperty("name", "Yasser");
        connection.authenticate(authParams, loginCallback);

        TestUtil.assertLastMessageWas(
                mockEndpoint,
                MessageBuilder.getMsg(Topic.AUTH, Actions.REQUEST, "{\"name\":\"Yasser\"}"));

        verifyConnectionState(ConnectionState.AUTHENTICATING);
    }

    @Test
    public void sendingAuthenticationWithNoParams() {
        this.challengeAck();
        JsonObject authParams = new JsonObject();
        connection.authenticate(authParams, loginCallback);

        TestUtil.assertLastMessageWas(
                mockEndpoint,
                MessageBuilder.getMsg(Topic.AUTH, Actions.REQUEST, "{}"));

        verifyConnectionState(ConnectionState.AUTHENTICATING);
    }

    @Test
    public void gettingValidAuthenticationBack() {
        sendingAuthentication();

        mockEndpoint.sendMessage(MessageBuilder.getMsg(Topic.AUTH, Actions.ACK));

        verifyConnectionState(ConnectionState.OPEN);
        verify(loginCallback, times(1)).loginSuccess(null);
    }

    @Test
    public void gettingInValidAuthenticationBack() {
        sendingAuthentication();

        mockEndpoint.sendMessage(MessageBuilder.getMsg(
                Topic.AUTH,
                Actions.ERROR,
                Event.NOT_AUTHENTICATED.toString(),
                "SFail"));

        verifyConnectionState(ConnectionState.AWAITING_AUTHENTICATION);
        verify(loginCallback, times(1)).loginFailed(Event.NOT_AUTHENTICATED, "Fail");
    }

    @Test
    public void errorsWhenTooManyAuthAttempts() {
        sendingAuthentication();

        mockEndpoint.sendMessage(MessageBuilder.getMsg(
                Topic.AUTH,
                Actions.ERROR,
                Event.TOO_MANY_AUTH_ATTEMPTS.toString(),
                "STOO_MANY_AUTH_ATTEMPTS"));

        verify(loginCallback, times(1))
                .loginFailed(Event.TOO_MANY_AUTH_ATTEMPTS, "TOO_MANY_AUTH_ATTEMPTS");

        JsonObject authParams = new JsonObject();
        authParams.addProperty("name", "Yasser");

        connection.authenticate(authParams, loginCallback);
        verify(deepstreamClientMock, times(1))
                .onError(Topic.ERROR, Event.IS_CLOSED, "The client's connection was closed");
    }

    @Test
    public void gettingValidAuthenticationBackWithData() {
        sendingAuthentication();

        JsonObject data = new JsonObject();
        data.addProperty("favouriteColour", "red");

        mockEndpoint.sendMessage(MessageBuilder.getMsg(
                Topic.AUTH, Actions.ACK, "O" + data.toString()));

        verifyConnectionState(ConnectionState.OPEN);
        verify(loginCallback, times(1)).loginSuccess(data);
    }

    private void verifyConnectionState(ConnectionState connectionState) {
        assertEquals(connection.getConnectionState(), connectionState);
        verify(connectionStateListenerMock, atLeastOnce()).connectionStateChanged(connectionState);
    }
}