
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
import java.util.concurrent.Callable;

/**
 * Registry of unique IDs for objects.
 *
 * <p>
 * Instances support creating unique {@code long} ID numbers for objects, as well as setting the unique ID
 * to a specific value for any unregistered object.
 *
 * <p>
 * This class uses object identity, not {@link Object#equals Object.equals()}, to distinguish objects.
 *
 * <p>
 * Weak references are used to ensure that registered objects can be garbage collected normally.
 *
 * <p>
 * New {@code long} ID numbers are issued serially; after 2<sup>64</sup>-1 invocations of {@link #getId getId()},
 * an {@link IllegalStateException} will be thrown.
 *
 * @see org.dellroad.stuff.jibx.IdMapper
 */
public class IdGenerator {

    private static final ThreadLocal<LinkedList<IdGenerator>> CURRENT = new ThreadLocal<LinkedList<IdGenerator>>();

    private final HashMap<Ref, Long> idMap = new HashMap<Ref, Long>();
    private final HashMap<Long, Ref> refMap = new HashMap<Long, Ref>();
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
     * <p>
     * New IDs are assigned sequentially starting at {@code 1}. No conflict avoidance with IDs assigned
     * via {@link #setId setId()} is performed; if there is a conflict, an exception is thrown.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws IllegalStateException if the next sequential ID has already been assigned to a different object
     *  via {@link #setId setId()}
     * @throws IllegalStateException if all 2<sup>64</sup>-1 values have been used up
     * @return a non-zero, unique identifier for {@code obj}
     */
    public synchronized long getId(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("null obj");
        this.flush();
        Ref ref = new Ref(obj, this.queue);
        Long id = this.idMap.get(ref);
        if (id == null) {
            if (this.next == 0)
                throw new IllegalStateException("no more identifiers left!");
            id = this.next++;
            this.idMap.put(ref, id);
            this.refMap.put(id, ref);
        }
        return id;
    }

    /**
     * Assign a unique ID to the given object. Does nothing if the object and ID number are already associated.
     *
     * @param obj object to assign
     * @param id unique ID number to assign
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws IllegalArgumentException if {@code id} has already been assigned to some other object
     */
    public synchronized void setId(Object obj, long id) {
        if (obj == null)
            throw new IllegalArgumentException("null obj");
        this.flush();
        Ref ref = this.refMap.get(id);
        if (ref != null) {
            if (ref.get() != obj)
                throw new IllegalArgumentException("id " + id + " is already assigned to another object");
            return;
        }
        ref = new Ref(obj, this.queue);
        this.idMap.put(ref, id);
        this.refMap.put(id, ref);
    }

    /**
     * Get the object assigned to the given ID.
     *
     * @param id unique ID
     * @return object associated with that ID, or null if no object is assigned to {@code id}
     */
    public synchronized Object getObject(long id) {
        this.flush();
        Ref ref = this.refMap.get(id);
        return ref != null ? ref.get() : null;
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
        Reference<? extends Object> entry;
        while ((entry = this.queue.poll()) != null) {
            Ref ref = (Ref)entry;
            Long id = this.idMap.get(ref);
            this.idMap.remove(ref);
            this.refMap.remove(id);
        }
    }

    /**
     * Create a new {@link IdGenerator} and make it available via {@link #get()} for the duration of the given operation.
     *
     * <p>
     * This method is re-entrant: nested invocations of this method in the same thread will cause new {@link IdGenerator}
     * instances to be created and used for the duration of the nested action. Therefore, don't nest invocations to this
     * method if you want the same instance to be used everywhere.
     *
     * @param action action to perform, and which may successfully invoke {@link #get}
     * @throws NullPointerException if {@code action} is null
     */
    public static void run(final Runnable action) {
        IdGenerator.push();
        try {
            action.run();
        } finally {
            IdGenerator.pop();
        }
    }

    /**
     * Create a new {@link IdGenerator} and make it available via {@link #get()} for the duration of the given operation.
     *
     * <p>
     * This method is re-entrant: nested invocations of this method in the same thread will cause new {@link IdGenerator}
     * instances to be created and used for the duration of the nested action. Therefore, don't nest invocations to this
     * method if you want the same instance to be used everywhere.
     *
     * @param action action to perform, and which may successfully invoke {@link #get}
     * @return result of invoking {@code action}
     * @throws NullPointerException if {@code action} is null
     */
    public static <R> R run(final Callable<R> action) throws Exception {
        IdGenerator.push();
        try {
            return action.call();
        } finally {
            IdGenerator.pop();
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
        IdGenerator current = IdGenerator.peek();
        if (current == null)
            throw new IllegalStateException("not running within an invocation of run()");
        return current;
    }

// Thread-local IdGenerator stack management

    private static void push() {
        LinkedList<IdGenerator> stack = IdGenerator.CURRENT.get();
        if (stack == null) {
            stack = new LinkedList<IdGenerator>();
            IdGenerator.CURRENT.set(stack);
        }
        stack.push(new IdGenerator());
    }

    private static void pop() {
        LinkedList<IdGenerator> stack = IdGenerator.CURRENT.get();
        assert stack != null;
        stack.pop();
        if (stack.isEmpty())
            IdGenerator.CURRENT.remove();
    }

    private static IdGenerator peek() {
        LinkedList<IdGenerator> stack = IdGenerator.CURRENT.get();
        if (stack == null)
            return null;
        assert !stack.isEmpty();
        return stack.peek();
    }

// Reference to a registered object that weakly references the actual object

    private static final class Ref extends WeakReference<Object> {

        private final int hashCode;

        Ref(Object obj, ReferenceQueue<Object> queue) {
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
            Ref that = (Ref)obj;
            obj = this.get();
            return obj != null ? obj == that.get() : false;
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }
}

