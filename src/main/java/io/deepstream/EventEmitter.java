package io.deepstream;

import com.google.j2objc.annotations.ObjectiveCName;

import java.util.List;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;


/**
 * The event emitter which is ported from the JavaScript module. This class is thread-safe.
 *
 * @see <a href="https://github.com/component/emitter">https://github.com/component/emitter</a>
 * @param <L> listener saved in this event emitter
 */
class EventEmitter<L> {

    private final ConcurrentMap<String, ConcurrentLinkedQueue<L>> callbacks = new ConcurrentHashMap<>();

    /**
     * @param enu eventName as an Enum
     * @param fn  The listener to invoke
     * @return a reference to this object.
     */
    @ObjectiveCName("on:fn:")
    public EventEmitter<L> on(Enum<?> enu, L fn) {
        this.on(enu.toString(), fn);
        return this;
    }

    /**
     * Listens on the event.
     *
     * @param event event name.
     * @param fn    The listener to invoke
     * @return a reference to this object.
     */
    @ObjectiveCName("onWithEvent:fn:")
    public EventEmitter<L> on(String event, L fn) {
        callbacks.computeIfAbsent(event, (String ignored) -> new ConcurrentLinkedQueue<>())
                .add(fn);
        return this;
    }

    /**
     * Removes the listener.
     *
     * @param event an event name.
     * @param fn    The listener to invoke
     * @return a reference to this object.
     */
    @ObjectiveCName("off:fn:")
    public EventEmitter<L> off(String event, L fn) {
        ConcurrentLinkedQueue<L> callbacks = this.callbacks.get(event);
        if (callbacks != null) {
            callbacks.remove(fn);
            if (callbacks.isEmpty()) {
                this.callbacks.remove(event);
            }
        }
        return this;
    }

    /**
     * Returns a list of listeners for the specified event.
     *
     * @param event an event name.
     * @return a reference to this object.
     */
    @ObjectiveCName("listeners:")
    public List<L> listeners(String event) {
        ConcurrentLinkedQueue<L> callbacks = this.callbacks.get(event);
        if (callbacks != null) {
            return new ArrayList<>(callbacks);
        }
        return Collections.emptyList();
    }

    /**
     * Check if this emitter has listeners for the specified event.
     *
     * @param event an event name.
     * @return true if a listener exists for that event name
     */
    @ObjectiveCName("hasListeners:")
    public boolean hasListeners(String event) {
        ConcurrentLinkedQueue<L> callbacks = this.callbacks.get(event);
        return callbacks == null || callbacks.isEmpty();
    }

    /**
     * Check if this emitter has any listeners
     *
     * @return all listeners
     */
    public Set<String> getEvents() {
        return this.callbacks.keySet();
    }

    /**
     * Check if this emitter has any listeners
     *
     * @return true if any listeners exist
     */
    public boolean hasListeners() {
        return this.callbacks.isEmpty();
    }
}