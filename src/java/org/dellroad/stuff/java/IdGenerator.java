
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Generates unique IDs for any object.
 *
 * <p>
 * This class uses object identity, not {@link Object#equals Object.equals()}, to distinguish objects.
 * Weak references are used to ensure that identified objects can be garbage collected normally.
 *
 * <p>
 * The {@code long} ID numbers are issued serially; after 2<sup>64</sup>-1 invocations of {@link #getId getId()},
 * an {@link IllegalStateException} will be thrown.
 *
 * @see org.dellroad.stuff.jibx.IdMapper
 */
public class IdGenerator {

    private static final ThreadLocal<LinkedList<IdGenerator>> CURRENT = new ThreadLocal<LinkedList<IdGenerator>>() {
        @Override
        public LinkedList<IdGenerator> initialValue() {
            return new LinkedList<IdGenerator>();
        }
    };

    private final HashMap<Object, Long> map = new HashMap<Object, Long>();
    private final ReferenceQueue<Object> queue = new ReferenceQueue<Object>();

    private long next = 1;

    /**
     * Get a unique ID for the given object.
     *
     * <p>
     * If this method has been previously invoked on this instance with the same {@code obj} parameter (where "same" means
     * object identity, not {@link Object#equals Object.equals()} identity), then the same ID value will be returned.
     * Otherwise a new ID value will be returned.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws IllegalStateException if all 2<sup>64</sup>-1 values have been used up
     * @return a non-zero, unique identifier for {@code obj}
     */
    public long getId(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("null obj");
        Key key = new Key(obj, this.queue);
        synchronized (this) {
            this.flush();
            Long id = this.map.get(key);
            if (id == null) {
                if (this.next == 0)
                    throw new IllegalStateException("no more identifiers left!");
                id = this.next++;
                this.map.put(key, id);
            }
            return id;
        }
    }

    /**
     * Flush any cleared weak references.
     *
     * <p>
     * This operation is invoked by {@link #getId getId()}, so it's not necessary to explicitly invoke it.
     * However, if a lot of previously ID'd objects have been garbage collected since the last call to
     * {@link #getId getId()}, then invoking this method may free up some additional memory.
     */
    public synchronized void flush() {
        Reference<? extends Object> key;
        while ((key = this.queue.poll()) != null)
            this.map.remove(key);
    }

    /**
     * Create a new {@link IdGenerator} and make it available via {@link #get()} for the duration of the given operation.
     *
     * <p>
     * This method is re-entrant: nested invocations of this method in the same thread will cause new {@link IdGenerator}
     * instances to be created and used for the duration of the nested action.
     *
     * @param action action to perform, and which may successfully invoke {@link #get}
     * @throws NullPointerException if {@code action} is null
     */
    public static void run(final Runnable action) {
        IdGenerator.CURRENT.get().push(new IdGenerator());
        try {
            action.run();
        } finally {
            IdGenerator.CURRENT.get().pop();
        }
    }

    /**
     * Get the {@link IdGenerator} associated with the current thread.
     * This method only works when the current thread is running within an invocation of {@link #run run()};
     * otherwise, an {@link IllegalStateException} is thrown.
     *
     * @return the {@link IdGenerator} created in the most recent, still running invocation of {@link #run} in this thread
     * @throws IllegalStateException if there is not such instance
     */
    public static IdGenerator get() {
        IdGenerator current = IdGenerator.CURRENT.get().peek();
        if (current == null)
            throw new IllegalStateException("not running within an invocation of run()");
        return current;
    }

    // Our hash key that weakly references the actual object
    private static final class Key extends WeakReference<Object> {

        private final int hashCode;

        Key(Object obj, ReferenceQueue<Object> queue) {
            super(obj, queue);
            if (obj == null)
                throw new IllegalArgumentException("null obj");
            this.hashCode = System.identityHashCode(obj);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            Key that = (Key)obj;
            obj = this.get();
            return obj != null ? obj == that.get() : false;
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }
}

