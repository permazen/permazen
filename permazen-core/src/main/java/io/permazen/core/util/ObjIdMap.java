
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import io.permazen.core.ObjId;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import javax.annotation.concurrent.NotThreadSafe;

import org.dellroad.stuff.util.LongMap;

/**
 * A map with {@link ObjId} keys.
 *
 * <p>
 * This implementation is space optimized for the 64-bits of information contained in an {@link ObjId}.
 * Instances do not accept null keys and are not thread safe.
 *
 * <p>
 * Instances are {@link Serializable} if the map values.
 */
@NotThreadSafe
public class ObjIdMap<V> extends AbstractMap<ObjId, V> implements Cloneable, Serializable {

    private static final long serialVersionUID = -4931628136892145405L;

    private /*final*/ LongMap<V> map;

// Constructors

    /**
     * Constructs an empty instance.
     */
    public ObjIdMap() {
        this(0);
    }

    /**
     * Constructs an instance with the given initial capacity.
     *
     * @param capacity initial capacity
     * @throws IllegalArgumentException if {@code capacity} is negative
     */
    public ObjIdMap(int capacity) {
        this.map = new LongMap<>(capacity);
    }

    /**
     * Constructs an instance initialized from the given map.
     *
     * @param map initial contents for this instance
     * @throws NullPointerException if {@code map} is null
     * @throws IllegalArgumentException if {@code map} contains a null key
     */
    @SuppressWarnings("this-escape")
    public ObjIdMap(Map<? extends ObjId, ? extends V> map) {
        this(map.size());
        for (Map.Entry<? extends ObjId, ? extends V> entry : map.entrySet())
            this.put(entry.getKey(), entry.getValue());
    }

// Methods

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public boolean containsKey(Object obj) {
        return obj instanceof ObjId && this.map.containsKey(((ObjId)obj).asLong());
    }

    @Override
    public V get(Object obj) {
        return obj instanceof ObjId ? this.map.get(((ObjId)obj).asLong()) : null;
    }

    @Override
    public V put(ObjId id, V value) {
        Preconditions.checkArgument(id != null, "null id");
        return this.map.put(id.asLong(), value);
    }

    @Override
    public V remove(Object obj) {
        return obj instanceof ObjId ? this.map.remove(((ObjId)obj).asLong()) : null;
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public ObjIdSet keySet() {
        return new ObjIdSet(this.map.keySet());
    }

    @Override
    public Set<Map.Entry<ObjId, V>> entrySet() {
        return new EntrySet(this.map.entrySet());
    }

    /**
     * Remove a single, arbitrary entry from this instance and return it.
     *
     * @return the removed entry, or null if this instance is empty
     */
    public Map.Entry<ObjId, V> removeOne() {
        final Map.Entry<Long, V> entry = this.map.removeOne();
        if (entry == null)
            return null;
        return new AbstractMap.SimpleImmutableEntry<>(new ObjId(entry.getKey()), entry.getValue());
    }

// Object

    // CHECKSTYLE OFF: EqualsHashCode
    @Override
    public int hashCode() {
        return this.entrySet().hashCode();              // this is more efficient than what superclass does
    }
    // CHECKSTYLE ON: EqualsHashCode

// Cloneable

    @Override
    @SuppressWarnings("unchecked")
    public ObjIdMap<V> clone() {
        final ObjIdMap<V> clone;
        try {
            clone = (ObjIdMap<V>)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.map = this.map.clone();
        return clone;
    }

// Package methods

    ObjIdMap<V> deepClone(UnaryOperator<V> valueCloner) {
        final ObjIdMap<V> clone = this.clone();
        for (Map.Entry<ObjId, V> entry : clone.entrySet())
            entry.setValue(valueCloner.apply(entry.getValue()));
        return clone;
    }

// EntrySet

    @SuppressWarnings("serial")
    class EntrySet extends AbstractSet<Map.Entry<ObjId, V>> {

        private final Set<Map.Entry<Long, V>> inner;

        EntrySet(Set<Map.Entry<Long, V>> inner) {
            this.inner = inner;
        }

        @Override
        public Iterator<Map.Entry<ObjId, V>> iterator() {
            return Iterators.transform(this.inner.iterator(), this::wrapEntry);
        }

        @Override
        public int size() {
            return this.inner.size();
        }

        @Override
        public boolean contains(Object obj) {
            return this.inner.contains(this.unwrapEntry(obj));
        }

        @Override
        public boolean remove(Object obj) {
            return this.inner.remove(this.unwrapEntry(obj));
        }

        @Override
        public void clear() {
            this.inner.clear();
        }

        // This works because ObjId.hashCode() == ObjId.asLong().hashCode()
        // CHECKSTYLE OFF: EqualsHashCode
        @Override
        public int hashCode() {
            return this.inner.hashCode();
        }
        // CHECKSTYLE ON: EqualsHashCode

        private Map.Entry<?, ?> unwrapEntry(Object obj) {
            if (!(obj instanceof Map.Entry))
                return null;
            final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)obj;
            final Object key = entry.getKey();
            if (!(key instanceof ObjId))
                return null;
            final ObjId id = (ObjId)key;
            return new AbstractMap.SimpleImmutableEntry<Object, Object>(id.asLong(), entry.getValue());
        }

        private Map.Entry<ObjId, V> wrapEntry(Map.Entry<Long, V> entry) {
            return new AbstractMap.SimpleEntry<ObjId, V>(new ObjId(entry.getKey()), entry.getValue()) {
                @Override
                public V setValue(V value) {
                    return entry.setValue(value);
                }
            };
        }
    }
}
