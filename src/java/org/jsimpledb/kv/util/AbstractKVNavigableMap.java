
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
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.KeyRangesUtil;
import org.jsimpledb.kv.SimpleKeyRanges;
import org.jsimpledb.util.AbstractIterationSet;
import org.jsimpledb.util.AbstractNavigableMap;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link java.util.NavigableMap} support superclass for maps backed by keys and values encoded as {@code byte[]}
 * keys and values in a {@link KVStore}, and whose key sort order is consistent with the {@code byte[]} key encoding.
 *
 * <p>
 * Subclasses must implement the {@linkplain #encodeKey encodeKey()}, {@linkplain #decodeKey decodeKey()},
 * and {@linkplain #decodeValue decodeValue()}, methods to convert keys and value to/from {@link KVStore} keys and values, and
 * {@link #createSubMap(boolean, KeyRanges, Bounds) createSubMap()} to allow creating reversed and restricted range sub-maps.
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
 * {@link #put put()}, {@link #remove remove()}, and {@link #clear}; note, these methods must verify the key
 * {@link #isVisible isVisible()} before making any changes.
 * </p>
 *
 * <p>
 * In addition to the normal min/max bounds check, instances support restricting the visible keys to those contained in a
 * configured {@link KeyRanges} instance; see {@link #restrictKeys restrictKeys()}.
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
     * Whether the ordering of this instance is reversed.
     */
    protected final boolean reversed;

    /**
     * Visible keys, or null if there are no restrictions.
     */
    protected final KeyRanges keyRanges;

// Constructors

    /**
     * Convenience constructor for when there are no range restrictions.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @throws IllegalArgumentException if {@code kv} is null
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode) {
        this(kv, prefixMode, (KeyRanges)null);
    }

    /**
     * Convenience constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix prefix defining minimum and maximum keys
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code prefix} is null or empty
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode, byte[] prefix) {
        this(kv, prefixMode, SimpleKeyRanges.forPrefix(prefix));
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param keyRanges restriction on visible keys, or null for none
     * @throws IllegalArgumentException if {@code kv} is null
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode, KeyRanges keyRanges) {
        this(kv, prefixMode, false, keyRanges, new Bounds<K>());
    }

    /**
     * Internal constructor. Used for creating sub-maps and reversed views.
     *
     * <p>
     * Note: if {@code bounds} are set, then {@code keyRanges} must exclude all keys outside of those bounds.
     * </p>
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted,
     *  but <i>not</i> {@code keyRanges}; note: means "absolutely" reversed, not relative to this instance
     * @param keyRanges restriction on visible keys, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code kv} or {@code bounds} is null
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode, boolean reversed, KeyRanges keyRanges, Bounds<K> bounds) {
        super(bounds);
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        this.kv = kv;
        this.prefixMode = prefixMode;
        this.reversed = reversed;
        this.keyRanges = keyRanges;
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
        return this.new KeySet(this.reversed, this.keyRanges, this.bounds);
    }

    /**
     * Create a view of this instance with additional {@code byte[]} key range restrictions applied.
     * The given {@link KeyRanges} restrictions will be added to the current restrictions (if any).
     * The {@link #bounds} associated with this instance will not change.
     *
     * @param keyRanges additional key restrictions to apply
     * @throws IllegalArgumentException if {@code keyRanges} is null
     */
    public NavigableMap<K, V> restrictKeys(KeyRanges keyRanges) {
        if (keyRanges == null)
            throw new IllegalArgumentException("null keyRanges");
        if (this.keyRanges != null)
            keyRanges = KeyRangesUtil.intersection(keyRanges, this.keyRanges);
        return this.createSubMap(this.reversed, keyRanges, this.bounds);
    }

    @Override
    protected boolean isWithinLowerBound(K key) {
        if (!super.isWithinLowerBound(key))
            return false;
        if (this.keyRanges == null)
            return true;
        final ByteWriter writer = new ByteWriter();
        this.encodeKey(writer, key);
        return this.keyRanges.nextLowerRange(writer.getBytes()) != null;
    }

    @Override
    protected boolean isWithinUpperBound(K key) {
        if (!super.isWithinUpperBound(key))
            return false;
        if (this.keyRanges == null)
            return true;
        final ByteWriter writer = new ByteWriter();
        this.encodeKey(writer, key);
        return this.keyRanges.nextHigherRange(writer.getBytes()) != null;
    }

    @Override
    protected final NavigableMap<K, V> createSubMap(boolean reverse, Bounds<K> newBounds) {

        // Determine the direction of the new sub-map
        final boolean newReversed = this.reversed ^ reverse;

        // Determine new min and max keys
        final KeyRanges newKeyRanges = this.buildKeyRanges(newReversed ? newBounds.reverse() : newBounds);

        // Create submap
        return this.createSubMap(newReversed, newKeyRanges, newBounds);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds.
     * The bounds are consistent with the reversed ordering (i.e., reversed if {@code reverse} is true)
     * and have already been range-checked against this instance's bounds.
     *
     * @param newReversed whether the new map's ordering should be reversed (implies {@code newBounds} are also inverted,
     *  but <i>not</i> {@code keyRanges}); note: means "absolutely" reversed, not relative to this instance
     * @param newKeyRanges new restriction on visible keys, or null for none
     * @param newBounds new bounds
     * @throws IllegalArgumentException if {@code newBounds} is null
     */
    protected abstract NavigableMap<K, V> createSubMap(boolean newReversed, KeyRanges newKeyRanges, Bounds<K> newBounds);

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
     * <p>
     * If not in prefix mode, all of {@code reader} must be consumed; otherwise, the consumed portion
     * is the prefix and any following keys with the same prefix are ignored.
     * </p>
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
     * Determine if the given {@code byte[]} key is visible in this set according to the configured {@link KeyRanges}.
     *
     * @see #restrictKeys restrictKeys()
     */
    protected boolean isVisible(byte[] key) {
        return this.keyRanges == null || this.keyRanges.contains(key);
    }

    /**
     * Encode the given key object, if possible, otherwise return null or throw an exception.
     * Delegates to {@link #encodeKey(ByteWriter, Object)} to attempt the actual encoding.
     *
     * @param obj key object to encode, possibly null
     * @param fail whether, if {@code obj} can't be encoded, to throw an exception (true) or return null (false)
     * @return encoed key for {@code obj}, or null if {@code fail} is false and {@code obj} has the wrong type or is out of bounds
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} has the wrong type
     * @throws IllegalArgumentException if {@code fail} is true and the resulting key is not {@linkplain #isVisible visible}
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
        if (!this.isVisible(key)) {
            if (fail)
                throw new IllegalArgumentException("value is out of bounds: " + obj);
            return null;
        }
        return key;
    }

    /**
     * Derive new {@link KeyRanges} from (possibly) new element bounds. The given bounds must <i>not</i> ever be reversed.
     */
    private KeyRanges buildKeyRanges(Bounds<K> bounds) {
        byte[] minKey;
        switch (bounds.getLowerBoundType()) {
        case NONE:
            minKey = null;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encodeKey(writer, bounds.getLowerBound());
            minKey = writer.getBytes();
            if (!bounds.getLowerBoundType().isInclusive())
                minKey = this.prefixMode ? ByteUtil.getKeyAfterPrefix(minKey) : ByteUtil.getNextKey(minKey);
            break;
        }
        byte[] maxKey;
        switch (bounds.getUpperBoundType()) {
        case NONE:
            maxKey = null;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encodeKey(writer, bounds.getUpperBound());
            maxKey = writer.getBytes();
            if (bounds.getUpperBoundType().isInclusive())
                maxKey = this.prefixMode ? ByteUtil.getKeyAfterPrefix(maxKey) : ByteUtil.getNextKey(maxKey);
            break;
        }
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) > 0)
            minKey = maxKey;
        KeyRanges newKeyRanges = new SimpleKeyRanges(minKey, maxKey);
        if (this.keyRanges != null)
            newKeyRanges = KeyRangesUtil.intersection(newKeyRanges, this.keyRanges);
        return newKeyRanges instanceof SimpleKeyRanges && ((SimpleKeyRanges)newKeyRanges).isFull() ? null : newKeyRanges;
    }

// KeySet

    private class KeySet extends AbstractKVNavigableSet<K> {

        KeySet(boolean reversed, KeyRanges keyRanges, Bounds<K> bounds) {
            super(AbstractKVNavigableMap.this.kv, AbstractKVNavigableMap.this.prefixMode, reversed, keyRanges, bounds);
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
        protected NavigableSet<K> createSubSet(boolean newReversed, KeyRanges newKeyRanges, Bounds<K> newBounds) {
            return AbstractKVNavigableMap.this.createSubMap(newReversed, newKeyRanges, newBounds).navigableKeySet();
        }
    }

// EntrySet

    private class EntrySet extends AbstractIterationSet<Map.Entry<K, V>> {

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new AbstractKVIterator<Map.Entry<K, V>>(AbstractKVNavigableMap.this.kv, AbstractKVNavigableMap.this.prefixMode,
              AbstractKVNavigableMap.this.reversed, AbstractKVNavigableMap.this.keyRanges) {

                @Override
                protected Map.Entry<K, V> decodePair(KVPair pair, ByteReader keyReader) {
                    final K key = AbstractKVNavigableMap.this.decodeKey(keyReader);
                    final V value = AbstractKVNavigableMap.this.decodeValue(pair);
                    return new MapEntry(key, value);
                }

                @Override
                protected void doRemove(Map.Entry<K, V> entry, KVPair pair) {
                    AbstractKVNavigableMap.this.entrySet().remove(entry);
                }
            };
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

