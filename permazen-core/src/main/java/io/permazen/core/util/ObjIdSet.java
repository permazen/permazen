
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import io.permazen.core.ObjId;
import io.permazen.util.ImmutableNavigableSet;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.LongStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.dellroad.stuff.util.LongSet;

/**
 * A set of {@link ObjId}s.
 *
 * <p>
 * This implementation is space optimized for the 64-bits of information contained in an {@link ObjId}.
 * Instances do not accept null values and are not thread safe.
 */
@NotThreadSafe
public class ObjIdSet extends AbstractSet<ObjId> implements Cloneable, Serializable {

    private static final long serialVersionUID = -8245070561628904938L;

    private /*final*/ LongSet set;

// Constructors

    /**
     * Constructs an empty instance.
     */
    public ObjIdSet() {
        this(new LongSet());
    }

    /**
     * Constructs an instance with the given initial capacity.
     *
     * @param capacity initial capacity
     * @throws IllegalArgumentException if {@code capacity} is negative
     */
    public ObjIdSet(int capacity) {
        this(new LongSet(capacity));
    }

    /**
     * Constructs an instance initialized with the given ID's.
     *
     * @param ids initial contents for this instance
     * @throws IllegalArgumentException if {@code ids} is null
     * @throws NullPointerException if any ID in {@code ids} is null
     */
    public ObjIdSet(Iterable<? extends ObjId> ids) {
        this(new LongSet());
        for (ObjId id : ids)
            this.add(id);
    }

    // Internal constructor
    ObjIdSet(LongSet set) {
        this.set = set;
    }

// Methods

    /**
     * Remove a single, arbitrary {@link ObjId} from this instance and return it.
     *
     * @return the removed {@link ObjId}, or null if this instance is empty
     */
    public ObjId removeOne() {
        final long value = this.set.removeOne();
        return value != 0 ? new ObjId(value) : null;
    }

    @Override
    public Iterator<ObjId> iterator() {
        return Iterators.transform(this.set.iterator(), ObjId::new);
    }

    @Override
    public int size() {
        return this.set.size();
    }

    @Override
    public boolean isEmpty() {
        return this.set.isEmpty();
    }

    @Override
    public boolean contains(Object obj) {
        return obj instanceof ObjId && this.set.contains(((ObjId)obj).asLong());
    }

    @Override
    public boolean add(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return this.set.add(id.asLong());
    }

    @Override
    public boolean remove(Object obj) {
        return obj instanceof ObjId && this.set.remove(((ObjId)obj).asLong());
    }

    @Override
    public void clear() {
        this.set.clear();
    }

    @Override
    public ObjId[] toArray() {
        return LongStream.of(this.set.toLongArray())
          .mapToObj(ObjId::new)
          .toArray(ObjId[]::new);
    }

    /**
     * Create a sorted, immutable snapshot of this instance.
     *
     * @return sorted, immutable snapshot
     */
    @SuppressWarnings("unchecked")
    public ImmutableNavigableSet<ObjId> sortedSnapshot() {
        final ObjId[] array = this.toArray();
        Arrays.sort(array);
        return new ImmutableNavigableSet<>(array, ObjId::compareTo);
    }

// Object

    @Override
    public int hashCode() {
        return this.set.hashCode();         // this works because ObjId.hashCode() == Long.hashCode(ObjId.asLong())
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
        clone.set = clone.set.clone();
        return clone;
    }
}
