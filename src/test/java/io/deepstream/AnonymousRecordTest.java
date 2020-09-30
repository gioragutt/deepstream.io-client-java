package io.deepstream;


import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.mockito.Mockito.*;

public class AnonymousRecordTest {

    private static final Gson GSON = new Gson();
    private static final String FIRST_RECORD_NAME = "firstRecordName";
    private static final String SECOND_RECORD_NAME = "secondRecordName";
    private static final String THIRD_RECORD_NAME = "thirdRecordName";

    private final MockDeepstreamClient deepstreamClientMock =
            TestUtil.createMockDeepstreamClient();
    private final MockConnection mockConnection =
            new MockConnection();
    private final RecordEventsListener recordEventsListener =
            mock(RecordEventsListener.class);
    private final AnonymousRecordNameChangedListener recordNameChangedListener =
            mock(AnonymousRecordNameChangedListener.class);
    private final RecordChangedCallback recordChangedCallback =
            mock(RecordChangedCallback.class);
    private final RecordPathChangedCallback recordPathChangedCallback =
            mock(RecordPathChangedCallback.class);

    RecordHandler recordHandler;
    AnonymousRecord anonymousRecord;

    private static JsonElement toJsonElement(String s) {
        return GSON.fromJson(s, JsonElement.class);
    }

    @Before
    public void setUp() throws InvalidDeepstreamConfig {
        Properties options = new Properties();

        options.put(ConfigOptions.SUBSCRIPTION_TIMEOUT.toString(), "10");
        options.put(ConfigOptions.RECORD_DELETE_TIMEOUT.toString(), "10");
        options.put(ConfigOptions.RECORD_READ_ACK_TIMEOUT.toString(), "10");
        options.put(ConfigOptions.RECORD_READ_TIMEOUT.toString(), "20");

        recordHandler = new RecordHandler(
                new DeepstreamConfig(options),
                mockConnection,
                deepstreamClientMock);
    }

    @Test
    public void recordHasCorrectDefaultState() {
        anonymousRecord = new AnonymousRecord(this.recordHandler);
        anonymousRecord.addRecordEventsListener(recordEventsListener);
        anonymousRecord.addRecordNameChangedListener(recordNameChangedListener);
    }

    @Test
    public void worksBeforeSetNameIsCalled() {
        recordHasCorrectDefaultState();

        Assert.assertNull(anonymousRecord.get());
        Assert.assertNull(anonymousRecord.name());

        anonymousRecord.addRecordEventsListener(recordEventsListener);
        anonymousRecord.subscribe(recordChangedCallback);
        anonymousRecord.subscribe("firstname", recordPathChangedCallback);

        verify(recordEventsListener, times(0))
                .onRecordDeleted(anyString());
        verify(recordEventsListener, times(0))
                .onRecordDiscarded(anyString());
        verify(recordChangedCallback, times(0))
                .onRecordChanged(anyString(), any(JsonElement.class));
        verify(recordPathChangedCallback, times(0))
                .onRecordPathChanged(anyString(), anyString(), any(JsonElement.class));
        verify(recordNameChangedListener, times(0))
                .recordNameChanged(anyString(), any(AnonymousRecord.class));

        Assert.assertNull(mockConnection.lastSentMessage);
    }

    @Test
    public void requestsARecordWhenSetNameIsCalled() {
        worksBeforeSetNameIsCalled();

        new Thread(() -> anonymousRecord.setName(FIRST_RECORD_NAME)).start();

        delayAndHandleRecordMessage("R|R|firstRecordName|1|{\"firstname\":\"Wolfram\"}");

        verify(recordNameChangedListener, times(1))
                .recordNameChanged(FIRST_RECORD_NAME, anonymousRecord);

        Assert.assertEquals(anonymousRecord.name(), FIRST_RECORD_NAME);
        Assert.assertEquals(
                mockConnection.lastSentMessage,
                TestUtil.formatMessage("R|CR|firstRecordName+"));

        verify(recordChangedCallback, times(1))
                .onRecordChanged(
                        FIRST_RECORD_NAME,
                        toJsonElement("{\"firstname\":\"Wolfram\"}"));

        verify(recordPathChangedCallback, times(1))
                .onRecordPathChanged(
                        FIRST_RECORD_NAME,
                        "firstname",
                        new JsonPrimitive("Wolfram"));
    }

    @Test
    public void updatesSubscriptionsOnceTheRecordIsReady() {
        requestsARecordWhenSetNameIsCalled();
    }

    @Test
    public void doesNotDoAnythingWhenAnotherRecordChanges() {
        updatesSubscriptionsOnceTheRecordIsReady();

        new Thread(() -> recordHandler.getRecord(SECOND_RECORD_NAME)).start();

        delayAndHandleRecordMessage("R|R|secondRecordName|2|{" +
                "\"firstname\":\"Egon\"," +
                "\"lastname\":\"Kowalski\"" +
                "}");

        verify(recordChangedCallback, times(1))
                .onRecordChanged(
                        FIRST_RECORD_NAME,
                        toJsonElement("{\"firstname\":\"Wolfram\"}"));

        verify(recordPathChangedCallback, times(1))
                .onRecordPathChanged(
                        FIRST_RECORD_NAME,
                        "firstname",
                        new JsonPrimitive("Wolfram"));
    }

    @Test
    public void movesSubscriptionsToOtherRecordWhenSetNameIsCalled() {
        doesNotDoAnythingWhenAnotherRecordChanges();
        resetMocks();

        anonymousRecord.setName(SECOND_RECORD_NAME);

        verify(recordChangedCallback, times(1))
                .onRecordChanged(
                        SECOND_RECORD_NAME,
                        toJsonElement("{\"firstname\":\"Egon\",\"lastname\":\"Kowalski\"}"));

        verify(recordPathChangedCallback, times(1))
                .onRecordPathChanged(
                        SECOND_RECORD_NAME,
                        "firstname",
                        new JsonPrimitive("Egon"));

        //TODO
        //recordHandler.handle( MessageParser.parseMessage( TestUtil.replaceSeperators( "R|A|D|firstRecordName" ), deepstreamClientMock ) );
        //verify(recordEventsListener, times(1) ).onRecordDiscarded( "recordA" );
    }

    @Test
    public void proxiesCallsThroughToTheUnderlyingRecord() {
        movesSubscriptionsToOtherRecordWhenSetNameIsCalled();
        resetMocks();

        Record secondRecord = recordHandler.getRecord(SECOND_RECORD_NAME);

        Assert.assertEquals(secondRecord.get("lastname"), new JsonPrimitive("Kowalski"));

        try {
            anonymousRecord.set("lastname", "Schrader");
        } catch (AnonymousRecordUninitialized anonymousRecordUninitialized) {
            anonymousRecordUninitialized.printStackTrace();
        }

        Assert.assertEquals(secondRecord.get("lastname"), new JsonPrimitive("Schrader"));
    }

    @Test
    public void notifiedNameChangedWhenSetNameIsCalled() {
        proxiesCallsThroughToTheUnderlyingRecord();
        resetMocks();

        new Thread(() -> anonymousRecord.setName(THIRD_RECORD_NAME)).start();

        delayAndHandleRecordMessage("R|R|thirdRecordName|1|{\"firstname\":\"Wolfram\"}");

        verify(recordNameChangedListener, times(1))
                .recordNameChanged(THIRD_RECORD_NAME, anonymousRecord);
    }

    private void delayAndHandleRecordMessage(String s) {
        try {
            Thread.sleep(50);
            recordHandler.handle(
                    MessageParser.parseMessage(TestUtil.formatMessage(s), deepstreamClientMock));
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void resetMocks() {
        reset(recordEventsListener);
        reset(recordChangedCallback);
    }
}
