
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import io.permazen.core.ObjId;

import java.util.NoSuchElementException;

/**
 * Factory methods for creating {@link ObjIdQueue}'s.
 */
public final class ObjIdQueues {

    private ObjIdQueues() {
    }

    /**
     * Create a new empty instance with first-in-first-out (FIFO) behavior.
     *
     * @return FIFO queue
     */
    public static ObjIdQueue fifo() {
        return new FifoQueue();
    }

    /**
     * Create a new instance with first-in-first-out (FIFO) behavior and initial contents copied from the given set.
     *
     * <p>
     * No particular ordering of the {@link ObjId}'s taken from {@code initial} is defined. To guarantee a specific
     * ordering, start with an empty instance.
     *
     * @param initial queue initial contents
     * @return FIFO queue
     * @throws IllegalArgumentException if {@code initial} is null
     */
    public static ObjIdQueue fifo(ObjIdSet initial) {
        Preconditions.checkArgument(initial != null, "null initial");
        return new FifoQueue(initial);
    }

    /**
     * Create a new empty instance with last-in-first-out (LIFO) behavior.
     *
     * @return LIFO queue
     */
    public static ObjIdQueue lifo() {
        return new LifoQueue();
    }

    /**
     * Create a new instance with last-in-first-out (LIFO) behavior and initial contents copied from the given set.
     *
     * <p>
     * No particular ordering of the {@link ObjId}'s taken from {@code initial} is defined. To guarantee a specific
     * ordering, start with an empty instance.
     *
     * @param initial queue initial contents
     * @return LIFO queue
     * @throws IllegalArgumentException if {@code initial} is null
     */
    public static ObjIdQueue lifo(ObjIdSet initial) {
        Preconditions.checkArgument(initial != null, "null initial");
        return new LifoQueue(initial);
    }

    /**
     * Create a new empty instance with no guaranted ordering.
     *
     * @return unordered queue
     */
    public static ObjIdQueue unordered() {
        return new UnorderedQueue();
    }

    /**
     * Create a new instance with no guaranted ordering and initial contents copied from the given set.
     *
     * @param initial queue initial contents
     * @return unordered queue
     * @throws IllegalArgumentException if {@code initial} is null
     */
    public static ObjIdQueue unordered(ObjIdSet initial) {
        Preconditions.checkArgument(initial != null, "null initial");
        return new UnorderedQueue(initial);
    }

// UnorderedQueue

    private static class UnorderedQueue implements ObjIdQueue {

        private final ObjIdSet ids;

    // Constructors

        UnorderedQueue() {
            this.ids = new ObjIdSet();
        }

        UnorderedQueue(ObjIdSet initial) {
            this.ids = initial.clone();
        }

    // ObjIdQueue

        @Override
        public void add(ObjId id) {
            this.ids.add(id);
        }

        @Override
        public ObjId next() {
            final ObjId id = this.ids.removeOne();
            if (id == null)
                throw new NoSuchElementException();
            return id;
        }

        @Override
        public final int size() {
            return this.ids.size();
        }

        @Override
        public final boolean isEmpty() {
            return this.ids.isEmpty();
        }
    }

// ArrayQueue

    private abstract static class ArrayQueue implements ObjIdQueue {

        protected long[] ids;
        protected int len;

    // Constructors

        ArrayQueue() {
            this.ids = new long[32];
        }

        ArrayQueue(ObjIdSet initial) {
            this.ids = initial.toLongArray();
            this.len = this.ids.length;
        }

        private void extendArray() {
            if (this.ids.length == Integer.MAX_VALUE)
                throw new IllegalStateException("cascade queue overflow");
            final int newLen = (int)Math.min((long)this.len * 2 + 13, (long)Integer.MAX_VALUE);
            final long[] newIds = new long[newLen];
            this.copyData(newIds);
            this.ids = newIds;
        }

    // ObjIdQueue

        @Override
        public final void add(ObjId id) {
            Preconditions.checkArgument(id != null, "null id");
            if (this.len == this.ids.length)
                this.extendArray();
            this.doAdd(id.asLong());
            this.len++;
        }

        @Override
        public final ObjId next() {
            if (this.len == 0)
                throw new NoSuchElementException();
            final long id = this.doNext();
            this.len--;
            return new ObjId(id);
        }

        @Override
        public final int size() {
            return this.len;
        }

        @Override
        public final boolean isEmpty() {
            return this.len == 0;
        }

    // Subclass Hooks

        protected abstract void doAdd(long id);

        protected abstract long doNext();

        protected abstract void copyData(long[] newIds);
    }

// LIFO queue

    private static class LifoQueue extends ArrayQueue {

    // Constructors

        LifoQueue() {
        }

        LifoQueue(ObjIdSet initial) {
            super(initial);
        }

    // ArrayQueue

        @Override
        protected void doAdd(long id) {
            this.ids[this.len] = id;
        }

        @Override
        protected long doNext() {
            return this.ids[this.len - 1];
        }

        @Override
        protected void copyData(long[] newIds) {
            System.arraycopy(this.ids, 0, newIds, 0, this.len);
        }
    }

// FIFO queue

    private static class FifoQueue extends ArrayQueue {

        private int off;

    // Constructors

        FifoQueue() {
        }

        FifoQueue(ObjIdSet initial) {
            super(initial);
        }

    // ArrayQueue

        @Override
        protected void doAdd(long id) {
            if (this.off == 0)
                this.off = this.ids.length;
            this.ids[--this.off] = id;
        }

        @Override
        protected long doNext() {
            return this.ids[(this.off + this.len - 1) % this.ids.length];
        }

        @Override
        protected void copyData(long[] newIds) {
            final int wrapped = (int)((long)this.off + (long)this.len - (long)this.ids.length);
            if (wrapped <= 0)
                System.arraycopy(this.ids, this.off, newIds, 0, this.len);
            else {
                final int unwrapped = this.len - wrapped;   // how many prior to overflow
                System.arraycopy(this.ids, this.off, newIds, 0, unwrapped);
                System.arraycopy(this.ids, 0, newIds, unwrapped, wrapped);
            }
            this.off = 0;
        }
    }
}
