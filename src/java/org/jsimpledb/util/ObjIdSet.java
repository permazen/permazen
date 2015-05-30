
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import java.util.AbstractSet;
import java.util.Iterator;

import org.jsimpledb.core.ObjId;

/**
 * A set of {@link ObjId}s.
 *
 * <p>
 * This implementation is space optimized for the 64-bits of information contained in an {@link ObjId}.
 * Instances do not accept null values and are not thread safe.
 * </p>
 */
public class ObjIdSet extends AbstractSet<ObjId> implements Cloneable {

    private /*final*/ ObjIdMap<?> map;

// Constructors

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
        this(new ObjIdMap<Void>(capacity, false));
    }

    /**
     * Constructs an instance initialized with the given ID's.
     *
     * @param ids initial contents for this instance
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

    // Internal constructor
    ObjIdSet(ObjIdMap<?> map) {
        this.map = map;
    }

// Methods

    @Override
    public Iterator<ObjId> iterator() {
        return new Iterator<ObjId>() {

            private final ObjIdMap<?>.EntrySetIterator entryIterator = ObjIdSet.this.map.new EntrySetIterator();

            @Override
            public boolean hasNext() {
                return this.entryIterator.hasNext();
            }

            @Override
            public ObjId next() {
                return this.entryIterator.next().getKey();
            }

            @Override
            public void remove() {
                this.entryIterator.remove();
            }
        };
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public boolean contains(Object obj) {
        return this.map.containsKey(obj);
    }

    @Override
    public boolean add(ObjId id) {
        if (this.map.containsKey(id))
            return false;
        this.map.put(id, null);
        return true;
    }

    @Override
    public boolean remove(Object obj) {
        if (!this.map.containsKey(obj))
            return false;
        this.map.remove(obj);
        return true;
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    /**
     * Produce a debug dump of this instance.
     */
    String debugDump() {
        return this.map.debugDump();
    }

// Object

    // This works because ObjId.hashCode() == ObjId.asLong().hashCode()
    @Override
    public int hashCode() {
        final long[] keyArray = this.map.getKeys();
        int hash = 0;
        for (int i = 0; i < keyArray.length; i++) {
            final long key = keyArray[i];
            if (key != 0)
                hash += (int)(key >>> 32) ^ (int)key;
        }
        return hash;
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
        clone.map = clone.map.clone();
        return clone;
    }
}

