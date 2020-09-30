package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

/**
 * Called whenever you try to perform an action on a record that has been discarded/deleted.
 * Retrieve a new instance of the record using {@link RecordHandler#getRecord(String)} to continue using it.
 */
public class DeepstreamRecordDestroyedException extends RuntimeException {
    /**
     * The name of the method that was attempted to be invoked
     */
    public final String method;

    @ObjectiveCName("init:")
    DeepstreamRecordDestroyedException(String method) {
        super(String.format(
                "record.%s called after record was discarded/deleted. " +
                        "Retrieve a new instance of the record to continue using it", method));
        this.method = method;
    }
}
