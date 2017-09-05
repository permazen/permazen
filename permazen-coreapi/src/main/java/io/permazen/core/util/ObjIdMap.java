
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import io.permazen.core.ObjId;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

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

    // Algorithm described here: http://en.wikipedia.org/wiki/Open_addressing

    private static final long serialVersionUID = -4931628136892145403L;

    private static final float EXPAND_THRESHOLD = 0.70f;    // expand array when > 70% full
    private static final float SHRINK_THRESHOLD = 0.25f;    // shrink array when < 25% full

    private static final int MIN_LOG2_LENGTH = 4;           // minimum array length = 16 slots
    private static final int MAX_LOG2_LENGTH = 30;          // maximum array length = 1 billion slots

    private long[] keys;                                    // has length always a power of 2
    private V[] values;                                     // will be null if we are being used to implement ObjIdSet
    private int size;                                       // the number of entries in the map
    private int log2len;                                    // log2 of keys.length and values.length (if not null)
    private int upperSizeLimit;                             // size threshold when to grow array
    private int lowerSizeLimit;                             // size threshold when to shrink array
    private int numHashShifts;                              // used by hash() function
    private volatile int modcount;

// Constructors

    /**
     * Constructs an empty instance.
     */
    public ObjIdMap() {
        this(0, true);
    }

    /**
     * Constructs an instance with the given initial capacity.
     *
     * @param capacity initial capacity
     * @throws IllegalArgumentException if {@code capacity} is negative
     */
    public ObjIdMap(int capacity) {
        this(capacity, true);
    }

    /**
     * Constructs an instance initialized from the given map.
     *
     * @param map initial contents for this instance
     * @throws NullPointerException if {@code map} is null
     * @throws IllegalArgumentException if {@code map} contains a null key
     */
    public ObjIdMap(Map<ObjId, ? extends V> map) {
        this(map.size(), true);
        for (Map.Entry<ObjId, ? extends V> entry : map.entrySet())
            this.put(entry.getKey(), entry.getValue());
    }

    // Internal constructor
    ObjIdMap(int capacity, boolean withValues) {
        Preconditions.checkArgument(capacity >= 0, "capacity < 0");
        capacity &= 0x3fffffff;                                                 // avoid integer overflow from large values
        capacity = (int)(capacity / EXPAND_THRESHOLD);                          // increase to account for overhead
        capacity = Math.max(1, capacity);                                       // avoid zero, on which the next line fails
        this.log2len = 32 - Integer.numberOfLeadingZeros(capacity - 1);         // round up to next power of 2
        this.log2len = Math.max(MIN_LOG2_LENGTH, this.log2len);                 // clip to bounds
        this.log2len = Math.min(MAX_LOG2_LENGTH, this.log2len);
        this.createArrays(withValues);
    }

// Methods

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean containsKey(Object obj) {

        // Check type
        if (!(obj instanceof ObjId))
            return false;
        final long value = ((ObjId)obj).asLong();
        assert value != 0;

        // Check slot for value
        final int slot = this.findSlot(value);
        if (this.keys[slot] == value)
            return true;
        assert this.keys[slot] == 0;
        return false;
    }

    @Override
    public V get(Object obj) {

        // Check type
        if (!(obj instanceof ObjId))
            return null;
        final long value = ((ObjId)obj).asLong();
        assert value != 0;

        // Check slot for value
        final int slot = this.findSlot(value);
        if (this.keys[slot] == value)
            return this.values != null ? this.values[slot] : null;
        assert this.keys[slot] == 0;
        return null;
    }

    @Override
    public V put(ObjId id, V value) {
        Preconditions.checkArgument(id != null, "null id");
        final long key = id.asLong();
        assert key != 0;
        return this.insert(key, value);
    }

    @Override
    public V remove(Object obj) {
        if (!(obj instanceof ObjId))
            return null;
        final long key = ((ObjId)obj).asLong();
        assert key != 0;
        return this.exsert(key);
    }

    @Override
    public void clear() {
        this.log2len = MIN_LOG2_LENGTH;
        this.createArrays(this.values != null);
        this.size = 0;
        this.modcount++;
    }

    @Override
    public ObjIdSet keySet() {
        return new ObjIdSet(this);
    }

    @Override
    public Set<Map.Entry<ObjId, V>> entrySet() {
        return new EntrySet();
    }

    /**
     * Remove a single, arbitrary entry from this instance and return it.
     *
     * @return the removed entry, or null if this instance is empty
     */
    public Map.Entry<ObjId, V> removeOne() {
        return this.removeOne(this.modcount * 11171);
    }

    private Map.Entry<ObjId, V> removeOne(final int offset) {
        if (this.size == 0)
            return null;
        final int mask = (1 << this.log2len) - 1;
        for (int i = 0; i < this.keys.length; i++) {
            final int slot = (offset + i) & mask;
            if (ObjIdMap.this.keys[slot] != 0) {
                final Entry entry = new Entry(slot);
                this.exsert(slot);
                return entry;
            }
        }
        return null;
    }

    /**
     * Produce a debug dump of this instance's keys.
     */
    String debugDump() {
        final StringBuilder buf = new StringBuilder();
        buf.append("OBJIDMAP: size=" + this.size + " len=" + this.keys.length + " modcount=" + this.modcount);
        for (int i = 0; i < this.keys.length; i++)
            buf.append('\n').append(String.format(" [%2d] %016x (hash %d)", i, this.keys[i], this.hash(this.keys[i])));
        return buf.toString();
    }

// Object

    @Override
    public int hashCode() {
        return this.entrySet().hashCode();              // this is more efficient than what superclass does
    }

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
        clone.keys = clone.keys.clone();
        if (clone.values != null)
            clone.values = clone.values.clone();
        return clone;
    }

// Package methods

    long[] getKeys() {
        return this.keys;
    }

    V getValue(int slot) {
        return this.values[slot];
    }

    void setValue(int slot, V value) {
        this.values[slot] = value;
    }

    ObjId[] toKeysArray() {
        final ObjId[] array = new ObjId[this.size];
        int index = 0;
        for (final long value : this.keys) {
            if (value != 0)
                array[index++] = new ObjId(value);
        }
        return array;
    }

// Internal methods

    private V insert(long key, V value) {

        // Find slot for key
        assert key != 0;
        final int slot = this.findSlot(key);

        // Key already exists? Just replace value
        if (this.keys[slot] == key) {
            if (this.values == null)
                return null;
            final V prev = this.values[slot];
            this.values[slot] = value;
            return prev;
        }

        // Insert new key/value pair
        assert this.keys[slot] == 0;
        this.keys[slot] = key;
        if (this.values != null) {
            assert this.values[slot] == null;
            this.values[slot] = value;
        }

        // Expand if necessary
        if (++this.size > this.upperSizeLimit && this.log2len < MAX_LOG2_LENGTH) {
            this.log2len++;
            this.resize();
        }
        this.modcount++;
        return null;
    }

    private V exsert(long key) {

        // Find slot for key
        final int slot = this.findSlot(key);
        if (this.keys[slot] == 0) {
            assert this.values == null || this.values[slot] == null;
            return null;
        }
        assert this.keys[slot] == key;

        // Remove key
        return this.exsert(slot);
    }

    private V exsert(final int slot) {

        // Sanity check
        assert this.keys[slot] != 0;
        final V ovalue = this.values != null ? this.values[slot] : null;

        // Remove key/value pair and fixup subsequent entries
        int i = slot;                                                   // i points to the new empty slot
        int j = slot;                                                   // j points to the next slot to fixup
loop:   while (true) {
            this.keys[i] = 0;
            if (this.values != null)
                this.values[i] = null;
            long jkey;
            V jvalue;
            while (true) {
                j = (j + 1) & (this.keys.length - 1);
                jkey = this.keys[j];
                if (jkey == 0)                                          // end of hash chain, no more fixups required
                    break loop;
                jvalue = this.values != null ? this.values[j] : null;   // get corresponding value
                final int k = this.hash(jkey);                          // find where jkey's hash chain started
                if (i <= j ? (i < k && k <= j) : (i < k || k <= j))     // jkey is between i and j, so it's not cut off
                    continue;
                break;                                                  // jkey is cut off from its hash chain, need to fix
            }
            this.keys[i] = jkey;                                        // move jkey back into its hash chain
            if (this.values != null)
                this.values[i] = jvalue;
            i = j;                                                      // restart fixups at jkey's old location
        }

        // Shrink if necessary
        if (--this.size < this.lowerSizeLimit && this.log2len > MIN_LOG2_LENGTH) {
            this.log2len--;
            this.resize();
        }
        this.modcount++;
        return ovalue;
    }

    private int findSlot(long value) {
        assert value != 0;
        int slot = this.hash(value);
        while (true) {
            final long existing = this.keys[slot];
            if (existing == 0 || existing == value)
                return slot;
            slot = (slot + 1) & (this.keys.length - 1);
        }
    }

    private int hash(long value) {
        final int shift = this.log2len;
        int hash = (int)value;
        for (int i = 0; i < this.numHashShifts; i++) {
            value >>>= shift;
            hash ^= (int)value;
        }
        return hash & (this.keys.length - 1);
    }

    private void resize() {

        // Grab a copy of old arrays and create new ones
        final long[] oldKeys = this.keys;
        final V[] oldValues = this.values;
        assert oldValues == null || oldValues.length == oldKeys.length;
        this.createArrays(oldValues != null);

        // Rehash key/value pairs from old array into new array
        for (int oldSlot = 0; oldSlot < oldKeys.length; oldSlot++) {
            final long key = oldKeys[oldSlot];
            if (key == 0) {
                assert oldValues == null || oldValues[oldSlot] == null;
                continue;
            }
            final int newSlot = this.findSlot(key);
            assert this.keys[newSlot] == 0;
            this.keys[newSlot] = key;
            if (this.values != null) {
                assert this.values[newSlot] == null;
                this.values[newSlot] = oldValues[oldSlot];
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void createArrays(boolean withValues) {
        assert this.log2len >= MIN_LOG2_LENGTH;
        assert this.log2len <= MAX_LOG2_LENGTH;
        final int arrayLength = 1 << this.log2len;
        this.lowerSizeLimit = this.log2len > MIN_LOG2_LENGTH ? (int)(SHRINK_THRESHOLD * arrayLength) : 0;
        this.upperSizeLimit = this.log2len < MAX_LOG2_LENGTH ? (int)(EXPAND_THRESHOLD * arrayLength) : arrayLength;
        this.numHashShifts = (64 + (this.log2len - 1)) / this.log2len;
        this.numHashShifts = Math.min(12, this.numHashShifts);
        this.keys = new long[arrayLength];
        if (withValues)
            this.values = (V[])new Object[arrayLength];
    }

// EntrySet

    class EntrySet extends AbstractSet<Map.Entry<ObjId, V>> {

        @Override
        public Iterator<Map.Entry<ObjId, V>> iterator() {
            return new EntrySetIterator();
        }

        @Override
        public int size() {
            return ObjIdMap.this.size;
        }

        @Override
        public boolean contains(Object obj) {
            if (!(obj instanceof Map.Entry))
                return false;
            final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)obj;
            final Object key = entry.getKey();
            final V actualValue = ObjIdMap.this.get(key);
            if (actualValue == null && !ObjIdMap.this.containsKey(key))
                return false;
            return entry.equals(new AbstractMap.SimpleEntry<>((ObjId)key, actualValue));
        }

        @Override
        public boolean remove(Object obj) {
            if (!(obj instanceof Map.Entry))
                return false;
            final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)obj;
            final Object key = entry.getKey();
            final V actualValue = ObjIdMap.this.get(key);
            if (actualValue == null && !ObjIdMap.this.containsKey(key))
                return false;
            if (actualValue != null ? actualValue.equals(entry.getValue()) : entry.getValue() == null) {
                ObjIdMap.this.remove(key);
                return true;
            }
            return false;
        }

        @Override
        public void clear() {
            ObjIdMap.this.clear();
        }

        // This works because ObjId.hashCode() == ObjId.asLong().hashCode()
        @Override
        public int hashCode() {
            final long[] keyArray = ObjIdMap.this.keys;
            final V[] valueArray = ObjIdMap.this.values;
            int hash = 0;
            for (int i = 0; i < keyArray.length; i++) {
                final long key = keyArray[i];
                if (key != 0) {
                    int entryHash = (int)(key >>> 32) ^ (int)key;
                    if (valueArray != null) {
                        final V value = valueArray[i];
                        if (value != null)
                            entryHash ^= value.hashCode();
                    }
                    hash += entryHash;
                }
            }
            return hash;
        }
    }

// EntrySetIterator

    class EntrySetIterator implements Iterator<Map.Entry<ObjId, V>> {

        private int modcount = ObjIdMap.this.modcount;
        private int removeSlot = -1;
        private int nextSlot;

        @Override
        public boolean hasNext() {
            return this.findNext(false) != -1;
        }

        @Override
        public Entry next() {
            final int slot = this.findNext(true);
            if (slot == -1)
                throw new NoSuchElementException();
            final long key = ObjIdMap.this.keys[slot];
            assert key != 0;
            this.removeSlot = slot;
            return new Entry(slot);
        }

        @Override
        public void remove() {
            if (this.removeSlot == -1)
                throw new IllegalStateException();
            if (this.modcount != ObjIdMap.this.modcount)
                throw new ConcurrentModificationException();
            ObjIdMap.this.exsert(this.removeSlot);
            this.removeSlot = -1;
            this.modcount++;                            // keep synchronized with ObjIdMap.this.modcount
        }

        private int findNext(boolean advance) {
            if (this.modcount != ObjIdMap.this.modcount)
                throw new ConcurrentModificationException();
            for (int slot = this.nextSlot; slot < ObjIdMap.this.keys.length; slot++) {
                if (ObjIdMap.this.keys[slot] == 0)
                    continue;
                this.nextSlot = advance ? slot + 1 : slot;
                return slot;
            }
            return -1;
        }
    }

// Entry

    @SuppressWarnings("serial")
    class Entry extends AbstractMap.SimpleEntry<ObjId, V> {

        private final int modcount = ObjIdMap.this.modcount;
        private final int slot;

        Entry(int slot) {
            super(new ObjId(ObjIdMap.this.keys[slot]), ObjIdMap.this.values != null ? ObjIdMap.this.values[slot] : null);
            this.slot = slot;
        }

        @Override
        public V setValue(V value) {
            if (ObjIdMap.this.modcount != this.modcount)
                throw new ConcurrentModificationException();
            if (ObjIdMap.this.values != null)
                ObjIdMap.this.values[this.slot] = value;
            return super.setValue(value);
        }
    }
}

