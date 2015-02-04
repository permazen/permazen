
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A set of {@link ObjId}s.
 *
 * <p>
 * Typically used when performing a copy operation of a (possibly) cyclic graph of object references,
 * to keep track of which objects have already been copied.
 * </p>
 *
 * <p>
 * This implementation is space optimized for the 64-bits of information contained in an {@link ObjId}.
 * Instances do not accept null values and are not thread safe.
 * </p>
 *
 * @see org.jsimpledb.JObject#copyTo JObject.copyTo()
 */
public class ObjIdSet extends AbstractSet<ObjId> implements Cloneable {

    // Algorithm described here: http://en.wikipedia.org/wiki/Open_addressing

    private static final int MIN_ARRAY_LENGTH = 0x00000008;
    private static final int MAX_ARRAY_LENGTH = 0x40000000;

    private long[] array;
    private int size;
    private volatile int modcount;

    /**
     * Constructs an empty instance.
     */
    public ObjIdSet() {
        this(0);
    }

    /**
     * Constructs an instance with the given initial capacity.
     *
     * @param capacity initial capacity
     * @throws IllegalArgumentException if {@code capacity} is negative
     */
    public ObjIdSet(int capacity) {
        if (capacity < 0)
            throw new IllegalArgumentException("capacity < 0");
        capacity &= 0x3fffffff;                                                     // avoid integer overflow from large values
        final int arrayLength = Math.max(capacity << 1, MIN_ARRAY_LENGTH);          // array length should be twice the capacity
        this.array = new long[arrayLength];
    }

    /**
     * Constructs an instance initialized with the given ID's.
     *
     * @throws IllegalArgumentException if {@code ids} is null
     * @throws NullPointerException if any ID in {@code ids} is null
     */
    public ObjIdSet(Iterable<? extends ObjId> ids) {
        this(0);
        if (ids == null)
            throw new IllegalArgumentException("null ids");
        for (ObjId id : ids)
            this.add(id);
    }

    @Override
    public Iterator<ObjId> iterator() {
        return new Iter();
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean contains(Object obj) {

        // Check type
        if (!(obj instanceof ObjId))
            return false;
        final long value = ((ObjId)obj).asLong();
        assert value != 0;

        // Check slot for value
        if (this.array[this.findSlot(value)] == value)
            return true;
        assert this.array[this.findSlot(value)] == 0;
        return false;
    }

    @Override
    public boolean add(ObjId id) {
        final long value = id.asLong();
        if (!this.insert(value))
            return false;
        if (++this.size > (this.array.length >> 1))             // expand when > 50% full
            this.expand();
        this.modcount++;
        return true;
    }

    @Override
    public boolean remove(Object obj) {
        if (!(obj instanceof ObjId))
            return false;
        final long value = ((ObjId)obj).asLong();
        assert value != 0;
        if (!this.exsert(value))
            return false;
        if (--this.size < (this.array.length >> 2))             // shrink when < 25% full
            this.shrink();
        this.modcount++;
        return true;
    }

    @Override
    public void clear() {
        this.array = new long[MIN_ARRAY_LENGTH];
        this.size = 0;
        this.modcount++;
    }

    /**
     * Produce a debug dump of this instance.
     */
    String debugDump() {
        final StringBuilder buf = new StringBuilder();
        buf.append("OBJIDSET: size=" + this.size + " len=" + this.array.length + " modcount=" + this.modcount);
        for (int i = 0; i < this.array.length; i++)
            buf.append('\n').append(String.format(" [%2d] %016x (hash %d)", i, this.array[i], this.hash(this.array[i])));
        return buf.toString();
    }

    /**
     * Force this instance to be equal to the given instance. All contents of this instance are discarded.
     * This method is more efficient than would be loading each element one-by-one.
     *
     * @throws IllegalArgumentException if {@code that} is null
     */
    public void copy(ObjIdSet that) {
        if (that == null)
            throw new IllegalArgumentException("null that");
        this.array = that.array.clone();
        this.size = that.size;
        this.modcount++;
    }

// Cloneable

    @Override
    public ObjIdSet clone() {
        final ObjIdSet clone;
        try {
            clone = (ObjIdSet)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.array = clone.array.clone();
        return clone;
    }

// Internal methods

    private boolean insert(long value) {

        // Find slot for value
        assert value != 0;
        final int index = this.findSlot(value);
        if (this.array[index] == value)
            return false;

        // Add value
        assert this.array[index] == 0;
        this.array[index] = value;
        return true;
    }

    private boolean exsert(long value) {

        // Find slot for value
        final int index = this.findSlot(value);
        if (this.array[index] == 0)
            return false;
        assert this.array[index] == value;

        // Remove value and fixup subsequent entries
        int i = index;                                                  // i points to the new empty slot
        int j = index;                                                  // j points to the next slot to fixup
        while (true) {
            this.array[i] = 0;
            long jvalue;
            while (true) {
                j = (j + 1) & (this.array.length - 1);
                jvalue = this.array[j];
                if (jvalue == 0)                                        // end of hash chain, no more fixups required
                    return true;
                final int k = this.hash(jvalue);                        // find where jvalue's hash chain started
                if (i <= j ? (i < k && k <= j) : (i < k || k <= j))     // jvalue is between i and j, so it's not cut off
                    continue;
                break;                                                  // jvalue is cut off from its hash chain
            }
            this.array[i] = jvalue;                                     // move jvalue back into its hash chain
            i = j;                                                      // restart fixups at jvalue's old location
        }
    }

    private int findSlot(long value) {
        assert value != 0;
        int index = this.hash(value) & (this.array.length - 1);
        while (true) {
            final long existing = this.array[index];
            if (existing == 0 || existing == value)
                return index;
            index = (index + 1) & (this.array.length - 1);
        }
    }

    private int hash(long value) {
        return ((int)value ^ (int)(value >>> 32)) & (this.array.length - 1);
    }

    private void expand() {
        final long[] oldarray = this.array;
        if (oldarray.length == MAX_ARRAY_LENGTH)
            return;
        this.array = new long[oldarray.length << 1];
        this.rehash(oldarray);
    }

    private void shrink() {
        final long[] oldarray = this.array;
        if (oldarray.length == MIN_ARRAY_LENGTH)
            return;
        this.array = new long[oldarray.length >> 1];
        this.rehash(oldarray);
    }

    private void rehash(long[] oldarray) {
        for (int i = 0; i < oldarray.length; i++) {
            final long value = oldarray[i];
            if (value != 0)
                this.insert(value);
        }
    }

// Iter

    private class Iter implements Iterator<ObjId> {

        private int modcount = ObjIdSet.this.modcount;
        private long removee;
        private int next;

        @Override
        public boolean hasNext() {
            return this.findNext(false) != -1;
        }

        @Override
        public ObjId next() {
            final int index = this.findNext(true);
            if (index == -1)
                throw new NoSuchElementException();
            final long value = ObjIdSet.this.array[index];
            assert value != 0;
            this.removee = value;
            return new ObjId(value);
        }

        @Override
        public void remove() {
            if (this.removee == 0)
                throw new IllegalStateException();
            if (this.modcount != ObjIdSet.this.modcount)
                throw new ConcurrentModificationException();
            ObjIdSet.this.remove(new ObjId(this.removee));
            this.modcount++;
            this.removee = 0;
        }

        private int findNext(boolean advance) {
            if (this.modcount != ObjIdSet.this.modcount)
                throw new ConcurrentModificationException();
            for (int index = this.next; index < ObjIdSet.this.array.length; index++) {
                if (ObjIdSet.this.array[index] != 0) {
                    this.next = advance ? index + 1 : index;
                    return index;
                }
            }
            return -1;
        }
    }
}

