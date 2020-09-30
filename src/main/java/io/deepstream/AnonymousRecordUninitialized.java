package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

/**
 * An exception that is thrown if you try to do a {@link AnonymousRecord#discard()} or {@link AnonymousRecord#delete()}
 * before a record has been set via {@link AnonymousRecord#setName(String)}
 */
public class AnonymousRecordUninitialized extends Exception {
    @ObjectiveCName("init:")
    AnonymousRecordUninitialized(String methodName) {
        super("Can`t invoke " + methodName + ". AnonymousRecord not initialised. Call setName first");
    }
}
