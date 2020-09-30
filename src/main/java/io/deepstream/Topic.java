package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

import java.util.Map;

/**
 * Provides the different topics that deepstream connections use. A user of this sdk will only need to access these
 * during error messages, such as via the {@link DeepstreamException}
 */
public enum Topic {
    /**
     * Connection topic, related to the first exchanges with server to validate connection
     */
    CONNECTION("C"),
    /**
     * Auth topic, related to the second exchange with server to authenticate
     * connection
     */
    AUTH("A"),
    /**
     * All generic errors arrive on this topic
     */
    ERROR("X"),
    /**
     * Event data is routed through this topic
     */
    EVENT("E"),
    /**
     * Record data is routed through this topic
     */
    RECORD("R"),
    /**
     * RPC data is routed through this topic
     */
    RPC("P"),
    /**
     * Presence data is routed through this topic
     */
    PRESENCE("U");

    private static final Map<String, Topic> LOOKUP = Utils.createEnumLookup(Topic.class);

    private final String topic;

    @ObjectiveCName("init:")
    Topic(String topic) {
        this.topic = topic;
    }

    @ObjectiveCName("getTopic:")
    static Topic getTopic(String topic) {
        return LOOKUP.get(topic);
    }

    @Override
    public String toString() {
        return topic;
    }
}
