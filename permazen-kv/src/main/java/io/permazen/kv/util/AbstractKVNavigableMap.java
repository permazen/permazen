
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyFilterUtil;
import io.permazen.kv.KeyRange;
import io.permazen.util.AbstractIterationSet;
import io.permazen.util.AbstractNavigableMap;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

/**
 * {@link java.util.NavigableMap} support superclass for maps backed by keys and values encoded as {@code byte[]}
 * arrays in a {@link KVStore}.
 *
 * <p>
 * The key sort order must be consistent with the corresponding key {@code ByteData} key encodings, i.e., unsigned lexicographical.
 *
 * <p>
 * There must be an equivalence between map keys and {@code byte[]} key encodings (i.e., there must be
 * only one valid encoding per map key).
 *
 * <p><b>Subclass Methods</b></p>
 *
 * <p>
 * Subclasses must implement the {@linkplain #encodeKey encodeKey()}, {@linkplain #decodeKey decodeKey()},
 * and {@linkplain #decodeValue decodeValue()} methods to convert keys and value to/from {@link KVStore} keys and values,
 * and {@link #createSubMap(boolean, KeyRange, KeyFilter, Bounds) createSubMap()}
 * to allow creating reversed and restricted range sub-maps.
 *
 * <p>
 * Subclasses must also implement {@link #comparator comparator()}, and the resulting sort order must be consistent with
 * the sort order of the encoded {@code byte[]} keys (possibly {@link #reversed}).
 *
 * <p>
 * This class provides a read-only implementation; for a mutable implementation, subclasses should also implement
 * {@link #put put()}, {@link #remove remove()}, and {@link #clear}; note, these methods must verify the key
 * {@link #isVisible isVisible()} before making any changes.
 *
 * <p>
 * Additional subclass notes:
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
 *
 * <p><b>Prefix Mode</b></p>
 *
 * <p>
 * Instances support "prefix mode" where the {@code byte[]} keys may have arbitrary trailing garbage, which is ignored,
 * and so by definition no key can be a prefix of any other key. The length of the prefix is determined implicitly by the
 * number of bytes produced by {@link #encodeKey encodeKey()} or consumed by {@link #decodeKey decodeKey()}.
 * When not in prefix mode, {@link #decodeKey decodeKey()} <b>must</b> consume the entire key to preserve correct semantics.
 *
 * <p>
 * Prefix mode can also be used when the map values (or some portion thereof) are contained in the key's "garbage" suffix.
 * The {@link #decodeValue decodeValue()} takes a {@link KVPair} so it can read from both the key and the value.
 * In particular, instances can be built from key/value data where all the values are empty.
 *
 * <p><b>Key Restrictions</b></p>
 *
 * <p>
 * Instances are configured with an (optional) {@link KeyRange}; when {@linkplain #bounds range restriction} is in
 * effect, this key range corresponds to the bounds.
 *
 * <p>
 * Instances also support filtering visible keys using a {@link KeyFilter}; see {@link #filterKeys filterKeys()}.
 * To be {@linkplain #isVisible} in the map, keys must both be in the {@link KeyRange} and pass the {@link KeyFilter}.
 *
 * <p><b>Concurrent Modifications</b></p>
 *
 * <p>
 * This implementation never throws {@link java.util.ConcurrentModificationException}; instead, iterators always
 * see the most up-to-date state of the associated {@link KVStore}.
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
     * Key range, or null for the entire range.
     */
    protected final KeyRange keyRange;

    /**
     * Key filter, or null if all keys in the range should be visible.
     */
    protected final KeyFilter keyFilter;

// Constructors

    /**
     * Convenience constructor for when there are no range restrictions.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @throws IllegalArgumentException if {@code kv} is null
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode) {
        this(kv, prefixMode, (KeyRange)null);
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
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode, ByteData prefix) {
        this(kv, prefixMode, KeyRange.forPrefix(prefix));
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param keyRange key range restriction, or null for none
     * @throws IllegalArgumentException if {@code kv} is null
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode, KeyRange keyRange) {
        this(kv, prefixMode, false, keyRange, null, new Bounds<>());
    }

    /**
     * Internal constructor. Used for creating sub-maps and reversed views.
     *
     * <p>
     * Note: if {@code bounds} are set, then {@code keyRange} must exclude all keys outside of those bounds.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted, but <i>not</i> {@code keyRange})
     * @param keyRange key range restriction, or null for none
     * @param keyFilter key filter, or null for none
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code kv} or {@code bounds} is null
     */
    protected AbstractKVNavigableMap(KVStore kv, boolean prefixMode, boolean reversed,
      KeyRange keyRange, KeyFilter keyFilter, Bounds<K> bounds) {
        super(bounds);
        Preconditions.checkArgument(kv != null, "null kv");
        this.kv = kv;
        this.prefixMode = prefixMode;
        this.reversed = reversed;
        this.keyRange = keyRange;
        this.keyFilter = keyFilter;
    }

    @Override
    public V get(Object obj) {

        // Encode key and check visibility
        final ByteData key = this.encodeVisibleKey(obj, false);
        if (key == null)
            return null;

        // Find key, or some longer key with the same prefix in prefix mode
        final KVPair pair;
        if (this.prefixMode) {
            ByteData maxKey;
            try {
                maxKey = ByteUtil.getKeyAfterPrefix(key);
            } catch (IllegalArgumentException e) {
                maxKey = null;
            }
            if ((pair = this.kv.getAtLeast(key, maxKey)) == null)
                return null;
            assert pair.getKey().startsWith(key);
        } else {
            final ByteData value = this.kv.get(key);
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
        return new KeySet();
    }

    /**
     * Create a view of this instance with additional filtering applied to the underlying {@code byte[]} keys.
     * Any map entry for which the corresponding key does not pass {@code keyFilter} will be effectively hidden from view.
     *
     * <p>
     * The restrictions of the given {@link KeyFilter} will be added to any current {@link KeyFilter} restrictions on this instance.
     * The {@link #bounds} associated with this instance will not change.
     *
     * @param keyFilter additional key filtering to apply
     * @return filtered view of this instance
     * @throws IllegalArgumentException if {@code keyFilter} is null
     */
    public NavigableMap<K, V> filterKeys(KeyFilter keyFilter) {
        Preconditions.checkArgument(keyFilter != null, "null keyFilter");
        if (this.keyFilter != null)
            keyFilter = KeyFilterUtil.intersection(keyFilter, this.keyFilter);
        return this.createSubMap(this.reversed, this.keyRange, keyFilter, this.bounds);
    }

    @Override
    protected boolean isWithinLowerBound(K key) {
        if (!super.isWithinLowerBound(key))
            return false;
        if (this.keyRange == null)
            return true;
        final ByteData.Writer writer = ByteData.newWriter();
        this.encodeKey(writer, key);
        return KeyRange.compare(writer.toByteData(), this.keyRange.getMin()) >= 0;
    }

    @Override
    protected boolean isWithinUpperBound(K key) {
        if (!super.isWithinUpperBound(key))
            return false;
        if (this.keyRange == null)
            return true;
        final ByteData.Writer writer = ByteData.newWriter();
        this.encodeKey(writer, key);
        return KeyRange.compare(writer.toByteData(), this.keyRange.getMax()) < 0;
    }

    @Override
    protected final NavigableMap<K, V> createSubMap(boolean reverse, Bounds<K> newBounds) {

        // Determine the direction of the new sub-map
        final boolean newReversed = this.reversed ^ reverse;

        // Determine new min and max keys
        final KeyRange newKeyRange = this.buildKeyRange(newReversed ? newBounds.reverse() : newBounds);

        // Create submap
        return this.createSubMap(newReversed, newKeyRange, this.keyFilter, newBounds);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds and
     * the given {@link KeyFilter}, if any.
     * The bounds are consistent with the reversed ordering (i.e., reversed if {@code reverse} is true)
     * and have already been range-checked against this instance's bounds.
     *
     * @param newReversed whether the new map's ordering should be reversed (implies {@code newBounds} are also inverted,
     *  but <i>not</i> {@code keyRange}); note: means "absolutely" reversed, not relative to this instance
     * @param newKeyRange new key range, or null for none; will be consistent with {@code bounds}, if any
     * @param newKeyFilter new key filter, or null for none
     * @param newBounds new bounds
     * @return restricted and/or filtered view of this instance
     * @throws IllegalArgumentException if {@code newBounds} is null
     */
    protected abstract NavigableMap<K, V> createSubMap(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<K> newBounds);

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
    protected abstract void encodeKey(ByteData.Writer writer, Object obj);

    /**
     * Decode a key object from an encoded {@code byte[]} key.
     *
     * <p>
     * If not in prefix mode, all of {@code reader} must be consumed; otherwise, the consumed portion
     * is the prefix and any following keys with the same prefix are ignored.
     *
     * @param reader input for encoded bytes
     * @return decoded map key
     */
    protected abstract K decodeKey(ByteData.Reader reader);

    /**
     * Decode a value object from an encoded {@code byte[]} key/value pair.
     *
     * @param pair key/value pair
     * @return decoded map value
     */
    protected abstract V decodeValue(KVPair pair);

    /**
     * Determine if the given {@code byte[]} key is visible in this map according to the configured
     * {@link KeyRange} and/or {@link KeyFilter}, if any.
     *
     * @param key key to test
     * @return true if key is visible
     * @throws IllegalArgumentException if {@code key} is null
     * @see #filterKeys filterKeys()
     */
    protected boolean isVisible(ByteData key) {
        return (this.keyRange == null || this.keyRange.contains(key))
          && (this.keyFilter == null || this.keyFilter.contains(key));
    }

    /**
     * Encode the given key object, if possible, and verify the corresponding {@code byte[]} key is visible,
     * otherwise return null or throw an exception.
     * Delegates to {@link #encodeKey(ByteData.Writer, Object)} to attempt the actual encoding.
     *
     * @param obj key object to encode, possibly null
     * @param fail whether, if {@code obj} can't be encoded, to throw an exception (true) or return null (false)
     * @return encoed key for {@code obj}, or null if {@code fail} is false and {@code obj} has the wrong type or is out of bounds
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} has the wrong type
     * @throws IllegalArgumentException if {@code fail} is true and the resulting key is not {@linkplain #isVisible visible}
     */
    protected ByteData encodeVisibleKey(Object obj, boolean fail) {
        final ByteData.Writer writer = ByteData.newWriter();
        try {
            this.encodeKey(writer, obj);
        } catch (IllegalArgumentException e) {
            if (!fail)
                return null;
            throw e;
        }
        final ByteData key = writer.toByteData();
        if (this.keyRange != null && !this.keyRange.contains(key)) {
            if (fail)
                throw new IllegalArgumentException(String.format("key is out of bounds: %s", obj));
            return null;
        }
        if (this.keyFilter != null && !this.keyFilter.contains(key)) {
            if (fail)
                throw new IllegalArgumentException(String.format("key is filtered out: %s", obj));
            return null;
        }
        return key;
    }

    /**
     * Derive a new {@link KeyRange} from (possibly) new element bounds. The given bounds must <i>not</i> ever be reversed.
     */
    private KeyRange buildKeyRange(Bounds<K> bounds) {
        final ByteData minKey = this.keyRange != null ? this.keyRange.getMin() : null;
        final ByteData maxKey = this.keyRange != null ? this.keyRange.getMax() : null;
        ByteData newMinKey;
        ByteData newMaxKey;
        switch (bounds.getLowerBoundType()) {
        case NONE:
            newMinKey = minKey;
            break;
        default:
            final ByteData.Writer writer = ByteData.newWriter();
            this.encodeKey(writer, bounds.getLowerBound());
            newMinKey = writer.toByteData();
            if (!bounds.getLowerBoundType().isInclusive())
                newMinKey = this.prefixMode ? ByteUtil.getKeyAfterPrefix(newMinKey) : ByteUtil.getNextKey(newMinKey);
            if (minKey != null)
                newMinKey = ByteUtil.max(newMinKey, minKey);
            if (maxKey != null)
                newMinKey = ByteUtil.min(newMinKey, maxKey);
            break;
        }
        switch (bounds.getUpperBoundType()) {
        case NONE:
            newMaxKey = maxKey;
            break;
        default:
            final ByteData.Writer writer = ByteData.newWriter();
            this.encodeKey(writer, bounds.getUpperBound());
            newMaxKey = writer.toByteData();
            if (bounds.getUpperBoundType().isInclusive())
                newMaxKey = this.prefixMode ? ByteUtil.getKeyAfterPrefix(newMaxKey) : ByteUtil.getNextKey(newMaxKey);
            if (maxKey != null)
                newMaxKey = ByteUtil.min(newMaxKey, maxKey);
            if (minKey != null)
                newMaxKey = ByteUtil.max(newMaxKey, minKey);
            break;
        }

        // Avoid creating an inverted key range
        if (newMinKey == null)
            newMinKey = ByteData.empty();
        else if (newMaxKey != null && newMinKey.compareTo(newMaxKey) > 0)
            newMaxKey = newMinKey;

        // Build KeyRange
        return new KeyRange(newMinKey, newMaxKey);
    }

// KeySet

    private class KeySet extends AbstractKVNavigableSet<K> {

        KeySet() {
            super(AbstractKVNavigableMap.this.kv, AbstractKVNavigableMap.this.prefixMode, AbstractKVNavigableMap.this.reversed,
              AbstractKVNavigableMap.this.keyRange, AbstractKVNavigableMap.this.keyFilter, AbstractKVNavigableMap.this.bounds);
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
        protected void encode(ByteData.Writer writer, Object obj) {
            AbstractKVNavigableMap.this.encodeKey(writer, obj);
        }

        @Override
        protected K decode(ByteData.Reader reader) {
            return AbstractKVNavigableMap.this.decodeKey(reader);
        }

        @Override
        protected NavigableSet<K> createSubSet(boolean newReversed,
          KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<K> newBounds) {
            return AbstractKVNavigableMap.this.createSubMap(newReversed, newKeyRange, newKeyFilter, newBounds).navigableKeySet();
        }
    }

// EntrySet

    private class EntrySet extends AbstractIterationSet<Map.Entry<K, V>> {

        @Override
        public CloseableIterator<Map.Entry<K, V>> iterator() {
            return new AbstractKVIterator<Map.Entry<K, V>>(AbstractKVNavigableMap.this.kv, AbstractKVNavigableMap.this.prefixMode,
              AbstractKVNavigableMap.this.reversed, AbstractKVNavigableMap.this.keyRange, AbstractKVNavigableMap.this.keyFilter) {

                @Override
                protected Map.Entry<K, V> decodePair(KVPair pair, ByteData.Reader keyReader) {
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
