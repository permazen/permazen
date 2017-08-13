
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.caching;

/**
 * An entry in a doubly-linked circular ring of entries, each with an associated owner.
 *
 * <p>
 * Instances are always a member of some ring. An instance is considered "detached" if it's in a ring of size one.
 *
 * @param <T> owner type
 */
class RingEntry<T> {

    private final T owner;

    private RingEntry<T> prev;
    private RingEntry<T> next;

    /**
     * Constructor.
     *
     * @param owner the owner of this entry
     */
    RingEntry(T owner) {
        this.owner = owner;
        this.prev = this;
        this.next = this;
    }

    /**
     * Get owner of this entry.
     */
    public T getOwner() {
        return this.owner;
    }

    /**
     * Get the next entry in the ring.
     *
     * @return next entry, or this entry if this entry is not attached to any other entries
     */
    public RingEntry<T> next() {
        assert this.checkValid();
        return this.next;
    }

    /**
     * Get the previous entry in the ring.
     *
     * @return previous entry, or this entry if this entry is not attached to any other entries
     */
    public RingEntry<T> prev() {
        assert this.checkValid();
        return this.prev;
    }

    /**
     * Determine whether this instance is attached to any other entries, or is alone in a ring of one.
     */
    public boolean isAttached() {
        assert this.checkValid();
        return this.prev != this;
    }

    /**
     * Detach this entry from any ring it's part of.
     *
     * <p>
     * If this instance is not attached to any other instances, then this method does nothing.
     * Otherwise, this instance is detached from the ring, and leaving the original ring minus
     * this instance, and a new ring of one containing only this instance.
     */
    public void detach() {
        assert this.checkValid();
        if (this.next == this) {
            assert this.prev == this;
            return;
        }
        this.prev.next = this.next;
        this.next.prev = this.prev;
        this.prev = this.next = this;
    }

    /**
     * Insert this instance into a ring.
     *
     * <p>
     * This method inserts this instance into the ring associated with {@code that}, immediately after {@code that}.
     * This instance is first detached from the ring it's currently in, if any.
     *
     * <p>
     * If {@code that} is this instance, no change occurs.
     */
    public void attachAfter(RingEntry<T> that) {
        assert this.checkValid();
        assert that.checkValid();
        if (this == that)
            return;
        if (this.next != this) {
            this.prev.next = this.next;
            this.next.prev = this.prev;
        } else
            assert this.prev == this;
        this.next = that.next;
        this.prev = that;
        this.next.prev = that.next = this;
    }

    private boolean checkValid() {
        assert (this.prev == this) == (this.next == this);
        assert this.prev.next == this;
        assert this.next.prev == this;
        int count = 0;
        for (RingEntry<T> that = this; (that = that.next) != this; count++)
            assert count < 100000 : "non-ring topology";
        return true;
    }
}
