
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.util.AbstractIterationSet;
import org.jsimpledb.util.AbstractNavigableMap;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.slf4j.LoggerFactory;

/**
 * {@link java.util.NavigableMap} support superclass for maps backed by keys and values encoded as {@code byte[]}
 * keys and values in a {@link KVStore}, and whose key sort order is consistent with the {@code byte[]} key encoding.
 *
 * <p>
 * Instances are configured with (optional) minimum and maximum keys; when {@linkplain #bounds range restriction} is in
 * effect, these minimum and maximum keys must correspond to the bounds. Subclasses must implement the
 * {@linkplain #encodeKey encodeKey()}, {@linkplain #decodeKey decodeKey()},
 * and {@linkplain #decodeValue decodeValue()}, methods to convert keys and value to/from {@link KVStore} keys and values, and
 * {@link #createSubMap(boolean, byte[], byte[], Bounds) createSubMap()} to allow creating reversed and restricted range sub-map.
 *
 * <p>
 * Subclasses must also implement {@link #comparator comparator()}, and the resulting sort order must be consistent with
 * the sort order of the encoded {@code byte[]} keys (possibly {@link #reversed}).
 * </p>
 *
 * <p>
 * Instances support "prefix mode" where the {@code byte[]} keys may have arbitrary trailing garbage, which is ignored,
 * and so by definition no key can be a prefix of any other key. The length of the prefix is determined implicitly by the
 * number of bytes produced by {@link #encodeKey encodeKey()} or consumed by {@link #decodeKey decodeKey()}.
 * When <b>not</b> in prefix mode, {@link #decodeKey decodeKey()} must consume the entire key (an error is logged if not).
 * </p>
 *
 * <p>
 * This class provides a read-only implementation; for a mutable implementation, subclasses should also implement
 * {@link #put put()}, {@link #remove remove()}, and {@link #clear}; note, these methods must be aware
 * of any {@linkplain AbstractNavigableMap#bounds range restrictions}.
 * </p>
 *
 * <p>
 * Notes on returned collection classes:
 * <ul>
 *  <li>{@link #navigableKeySet} returns a {@link Set} for which:
 *  <ul>
 *      <li>The {@link Set#add add()} method is not supported.</li>
 *      <li>The {@link Set#remove remove()} method and the {@link Iterator#remove remove()} method of the
 *          associated {@link Iterator} delegate to this instance's {@link #remove remove()} method.</li>
 *      <li>The {@link Set#clear clear()} method delegates to this instance's {@link #clear} method.</li>
 *  </ul>
 *  <li>{@link #entrySet} returns a {@link Set} for which:
 *  <ul>
 *      <li>The {@link Set#add add()} method is not supported.</li>
 *      <li>The {@link Set#remove remove()} method and the {@link Iterator#remove remove()} method of the
 *          associated {@link Iterator} delegate to this instance's {@link #remove remove()} method (but
 *          only when the {@link java.util.Map.Entry} is still contained in the map).
 *      <li>The {@link java.util.Map.Entry} elements' {@link java.util.Map.Entry#setValue setValue()} method delegates to
 *          this instance's {@link #put put()} method (but only when the {@link java.util.Map.Entry} is still contained in the map).
 *      <li>The {@link Set#clear clear()} method delegates to this instance's {@link #clear} method.</li>
 *  </ul>
 * </ul>
 * </p>
 *
 * <p>
 * This implementation never throws {@link java.util.ConcurrentModificationException}; instead, iterators always
 * see the most up-to-date state of the associated {@link KVStore}.
 * </p>
 *
 * @see AbstractKVNavigableSet
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("serial")
public abstract class AbstractKVNavigableMap<K, V> extends AbstractNavigableMap<K, V> {

    /**
     * The underlying {@link KVStore}.
     */
    protected final KVStore kv;

    /**
     * Whether we are in "prefix" mode.
     */
    protected final boolean prefixMode;

    /**
     * Whether the ordering of this instance is reversed (i.e., from {@link #maxKey} down to {@link #minKey}).
     */
    protected final boolean reversed;

    /**
     * Minimum visible key (inclusive), or null for no minimum. Always less than or equal to {@link #maxKey},
     * even if this instance is {@link #reversed}. Corresponds to the current {@link #bounds}, if any.
     */
    protected final byte[] minKey;

    /**
     * Maximum visible key (exclusive), or null for no maximum. Always greater than or equal to {@link #minKey},
     * even if this instance is {@link #reversed}. Corresponds to the current {@link #bounds}, if any.
     */
    protected final byte[] maxKey;

// Constructors

    /**
     * Convenience constructor for when there are no range restrictions.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public AbstractKVNavigableMap(KVStore kv, boolean prefixMode) {
        this(kv, prefixMode, null, null);
    }

    /**
     * Convenience constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix prefix defining minimum and maximum keys
     * @throws NullPointerException if {@code prefix} is null
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public AbstractKVNavigableMap(KVStore kv, boolean prefixMode, byte[] prefix) {
        this(kv, prefixMode, prefix, ByteUtil.getKeyAfterPrefix(prefix));
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param minKey minimum visible key (inclusive), or null for none
     * @param maxKey maximum visible key (exclusive), or null for none
     * @throws IllegalArgumentException if {@code minKey} and {@code maxKey} are both not null but {@code minKey > maxKey}
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public AbstractKVNavigableMap(KVStore kv, boolean prefixMode, byte[] minKey, byte[] maxKey) {
        this(kv, prefixMode, false, minKey, maxKey, new Bounds<K>());
    }

    /**
     * Internal constructor. Used for creating sub-maps and reversed views.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted,
     *  but <i>not</i> {@code minKey} and {@code maxKey}); note: means "absolutely" reversed, not relative to this instance
     * @param minKey minimum visible key (inclusive), or null for none; corresponds to {@code bounds}, if any
     * @param maxKey maximum visible key (exclusive), or null for none; corresponds to {@code bounds}, if any
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code kv} or {@code bounds} is null
     * @throws IllegalArgumentException if {@code minKey} and {@code maxKey} are both not null but {@code minKey > maxKey}
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode,
      boolean reversed, byte[] minKey, byte[] maxKey, Bounds<K> bounds) {
        super(bounds);
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        this.kv = kv;
        this.prefixMode = prefixMode;
        this.reversed = reversed;
        this.minKey = minKey != null ? minKey.clone() : null;
        this.maxKey = minKey != null ? maxKey.clone() : null;
        if (this.minKey != null && this.maxKey != null && ByteUtil.compare(this.minKey, this.maxKey) > 0)
            throw new IllegalArgumentException("minKey > maxKey");
    }

    @Override
    public V get(Object obj) {

        // Encode key
        final byte[] key = this.encodeKey(obj, false);
        if (key == null)
            return null;

        // Find key, or some longer key with the same prefix in prefix mode
        final KVPair pair;
        if (this.prefixMode) {
            pair = this.kv.getAtLeast(key);
            if (pair == null || !ByteUtil.isPrefixOf(key, pair.getKey()))
                return null;
        } else {
            final byte[] value = this.kv.get(key);
            if (value == null)
                return null;
            pair = new KVPair(key, value);
        }

        // Decode value
        return this.decodeValue(pair);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return this.new EntrySet();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return this.new KeySet(this.reversed, this.minKey, this.maxKey, this.bounds);
    }

    @Override
    protected final NavigableMap<K, V> createSubMap(boolean reverse, Bounds<K> newBounds) {

        // Determine the direction of the new sub-map
        final boolean newReversed = this.reversed ^ reverse;

        // Determine new min and max keys
        final byte[][] newMinMax = this.buildMinMax(newReversed ? newBounds.reverse() : newBounds);

        // Create submap
        return this.createSubMap(newReversed, newMinMax[0], newMinMax[1], newBounds);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds.
     * The bounds are consistent with the reversed ordering (i.e., reversed if {@code reverse} is true)
     * and have already been range-checked against this instance's bounds.
     *
     * @param newReversed whether the new map's ordering should be reversed (implies {@code newBounds} are also inverted,
     *  but <i>not</i> {@code newMinKey} and {@code newMaxKey}); note: means "absolutely" reversed, not relative to this instance
     * @param newMinKey new minimum visible key (inclusive), or null for none; corresponds to {@code bounds}, if any
     * @param newMaxKey new maximum visible key (exclusive), or null for none; corresponds to {@code bounds}, if any
     * @param newBounds new bounds
     * @throws IllegalArgumentException if {@code newBounds} is null
     */
    protected abstract NavigableMap<K, V> createSubMap(boolean newReversed,
      byte[] newMinKey, byte[] newMaxKey, Bounds<K> newBounds);

    /**
     * Encode the given key object into a {@code byte[]} key.
     * Note that this method must throw {@link IllegalArgumentException}, not {@link ClassCastException}
     * or {@code NullPointerException}, if {@code obj} does not have the correct type or is an illegal null value.
     *
     * @param writer output for encoded {@code byte[]} key corresponding to {@code obj}
     * @param obj map key object
     * @throws IllegalArgumentException if {@code obj} is not of the required Java type supported by this set
     * @throws IllegalArgumentException if {@code obj} is null and this set does not support null elements
     */
    protected abstract void encodeKey(ByteWriter writer, Object obj);

    /**
     * Decode a key object from an encoded {@code byte[]} key.
     *
     * @param reader input for encoded bytes
     * @return decoded map key
     */
    protected abstract K decodeKey(ByteReader reader);

    /**
     * Decode a value object from an encoded {@code byte[]} key/value pair.
     *
     * @param pair key/value pair
     * @return decoded map value
     */
    protected abstract V decodeValue(KVPair pair);

    /**
     * Determine if the given key is in this set's key range (i.e., between {@link #minKey} and {@link #maxKey}).
     */
    protected boolean inRange(byte[] key) {
        if (this.minKey != null && ByteUtil.compare(key, this.minKey) < 0)
            return false;
        if (this.maxKey != null && ByteUtil.compare(key, this.maxKey) >= 0)
            return false;
        return true;
    }

    /**
     * Encode the given key object, if possible, otherwise return null or throw an exception.
     * Delegates to {@link #encodeKey(ByteWriter, Object)} to attempt the actual encoding.
     *
     * @param obj key object to encode, possibly null
     * @param fail whether, if {@code obj} can't be encoded, to throw an exception (true) or return null (false)
     * @return encoed key for {@code obj}, or null if {@code fail} is false and {@code obj} has the wrong type or is out of bounds
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} has the wrong type
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} is out of bounds
     */
    protected byte[] encodeKey(Object obj, boolean fail) {
        final ByteWriter writer = new ByteWriter();
        try {
            this.encodeKey(writer, obj);
        } catch (IllegalArgumentException e) {
            if (!fail)
                return null;
            throw e;
        }
        final byte[] key = writer.getBytes();
        if (!this.inRange(key)) {
            if (fail)
                throw new IllegalArgumentException("value is out of bounds: " + obj);
            return null;
        }
        return key;
    }

    /**
     * Derive new min and max keys from (possibly) new element bounds. The given bounds must <i>not</i> ever be reversed.
     */
    private byte[][] buildMinMax(Bounds<K> bounds) {
        final byte[][] result = new byte[2][];
        switch (bounds.getLowerBoundType()) {
        case NONE:
            result[0] = this.minKey;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encodeKey(writer, bounds.getLowerBound());
            result[0] = writer.getBytes();
            if (!bounds.getLowerBoundType().isInclusive())
                result[0] = this.prefixMode ? ByteUtil.getKeyAfterPrefix(result[0]) : ByteUtil.getNextKey(result[0]);
            break;
        }
        switch (bounds.getUpperBoundType()) {
        case NONE:
            result[1] = this.maxKey;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encodeKey(writer, bounds.getUpperBound());
            result[1] = writer.getBytes();
            if (bounds.getUpperBoundType().isInclusive())
                result[1] = this.prefixMode ? ByteUtil.getKeyAfterPrefix(result[1]) : ByteUtil.getNextKey(result[1]);
            break;
        }
        return result;
    }

// KeySet

    private class KeySet extends AbstractKVNavigableSet<K> {

        KeySet(boolean reversed, byte[] minKey, byte[] maxKey, Bounds<K> bounds) {
            super(AbstractKVNavigableMap.this.kv, AbstractKVNavigableMap.this.prefixMode, reversed, minKey, maxKey, bounds);
        }

        @Override
        public Comparator<? super K> comparator() {
            return AbstractKVNavigableMap.this.comparator();
        }

        @Override
        public boolean remove(Object obj) {
            final boolean existed = AbstractKVNavigableMap.this.containsKey(obj);
            AbstractKVNavigableMap.this.remove(obj);
            return existed;
        }

        @Override
        public void clear() {
            AbstractKVNavigableMap.this.clear();
        }

        @Override
        protected void encode(ByteWriter writer, Object obj) {
            AbstractKVNavigableMap.this.encodeKey(writer, obj);
        }

        @Override
        protected K decode(ByteReader reader) {
            return AbstractKVNavigableMap.this.decodeKey(reader);
        }

        @Override
        protected NavigableSet<K> createSubSet(boolean newReversed, byte[] newMinKey, byte[] newMaxKey, Bounds<K> newBounds) {
            return AbstractKVNavigableMap.this.createSubMap(newReversed, newMinKey, newMaxKey, newBounds).navigableKeySet();
        }
    }

// EntrySet

    private class EntrySet extends AbstractIterationSet<Map.Entry<K, V>> {

        @Override
        public EntrySetIterator iterator() {
            return AbstractKVNavigableMap.this.new EntrySetIterator();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(Object obj) {

            // Check type
            if (!(obj instanceof Map.Entry))
                return false;
            final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)obj;

            // Find key
            if (!AbstractKVNavigableMap.this.containsKey(entry.getKey()))
                return false;

            // Compare key/value pair
            final K key = (K)entry.getKey();
            final V value = AbstractKVNavigableMap.this.get(entry.getKey());
            return new MapEntry(key, value).equals(entry);
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean remove(Object obj) {

            // Check type
            if (!(obj instanceof Map.Entry))
                return false;
            final Map.Entry<?, ?> entry = (Map.Entry<?, ?>)obj;

            // Find key
            if (!AbstractKVNavigableMap.this.containsKey(entry.getKey()))
                return false;

            // Compare key/value pair and remove entry (if contained)
            final K key = (K)entry.getKey();
            final V value = AbstractKVNavigableMap.this.get(entry.getKey());
            if (new MapEntry(key, value).equals(entry)) {
                AbstractKVNavigableMap.this.remove(key);
                return true;
            }

            // Not found
            return false;
        }

        @Override
        public void clear() {
            AbstractKVNavigableMap.this.clear();
        }
    }

// EntrySetIterator

    private class EntrySetIterator implements Iterator<Map.Entry<K, V>> {

        private final Iterator<KVPair> pairIterator;
        private Map.Entry<K, V> removeEntry;

        public EntrySetIterator() {
            if (AbstractKVNavigableMap.this.prefixMode) {
                this.pairIterator = new KVPairIterator(AbstractKVNavigableMap.this.kv,
                  AbstractKVNavigableMap.this.minKey, AbstractKVNavigableMap.this.maxKey, AbstractKVNavigableMap.this.reversed);
            } else {
                this.pairIterator = AbstractKVNavigableMap.this.kv.getRange(
                  AbstractKVNavigableMap.this.minKey, AbstractKVNavigableMap.this.maxKey, AbstractKVNavigableMap.this.reversed);
            }
        }

        @Override
        public boolean hasNext() {
            return this.pairIterator.hasNext();
        }

        @Override
        public Map.Entry<K, V> next() {
            final KVPair pair = this.pairIterator.next();

            // Decode key
            final ByteReader reader = new ByteReader(pair.getKey());
            final K key = AbstractKVNavigableMap.this.decodeKey(reader);
            if (!AbstractKVNavigableMap.this.prefixMode && reader.remain() > 0) {
                LoggerFactory.getLogger(this.getClass()).error(AbstractKVNavigableMap.this.getClass().getName()
                  + "@" + Integer.toHexString(System.identityHashCode(AbstractKVNavigableMap.this))
                  + ": " + reader.remain() + " undecoded bytes remain in key " + ByteUtil.toString(pair.getKey()) + " -> " + key);
            }

            // Decode value
            final V value = AbstractKVNavigableMap.this.decodeValue(pair);

            // In prefix mode, skip over any additional keys having the same prefix as what we just decoded
            if (AbstractKVNavigableMap.this.prefixMode) {
                final KVPairIterator kvPairIterator = (KVPairIterator)this.pairIterator;
                final byte[] prefix = reader.getBytes(0, reader.getOffset());
                kvPairIterator.setNextTarget(kvPairIterator.isReverse() ? prefix : ByteUtil.getKeyAfterPrefix(prefix));
            }

            // Save elment for possible remove()
            this.removeEntry = new MapEntry(key, value);
            return removeEntry;
        }

        @Override
        public void remove() {
            if (this.removeEntry == null)
                throw new IllegalStateException();
            AbstractKVNavigableMap.this.entrySet().remove(this.removeEntry);
            this.removeEntry = null;
        }
    }

// MapEntry

    private class MapEntry extends AbstractMap.SimpleEntry<K, V> {

        MapEntry(K key, V value) {
            super(key, value);
        }

        @Override
        public V setValue(V value) {
            AbstractKVNavigableMap.this.put(this.getKey(), value);
            return super.setValue(value);
        }
    }
}

