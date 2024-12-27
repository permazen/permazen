
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
import io.permazen.util.AbstractNavigableSet;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.NavigableSet;
import java.util.NoSuchElementException;

/**
 * {@link java.util.NavigableSet} support superclass for sets backed by elements encoded as {@code byte[]}
 * array keys in a {@link KVStore}.
 *
 * <p>
 * The key sort order must be consistent with the corresponding key {@code ByteData} key encodings, i.e., unsigned lexicographical.
 *
 * <p>
 * There must be an equivalence between elements and {@code byte[]} key encodings (i.e., there must be
 * only one valid encoding per set element). The values in the {@link KVStore} are ignored.
 *
 * <p><b>Subclass Methods</b></p>
 *
 * <p>
 * Subclasses must implement the {@linkplain #encode(ByteData.Writer, Object) encode()} and {@linkplain #decode decode()}
 * methods to convert elements to/from {@code byte[]} keys (associated values are ignored), and
 * {@link #createSubSet(boolean, KeyRange, KeyFilter, Bounds) createSubSet()}
 * to allow creating reversed and restricted range sub-sets.
 *
 * <p>
 * Subclasses must also implement {@link #comparator comparator()}, and the resulting sort order must be consistent with
 * the sort order of the encoded {@code byte[]} keys (possibly {@link #reversed}).
 *
 * <p>
 * This class provides a read-only implementation; for a mutable implementation, subclasses should also implement
 * {@link #add add()} (if appropriate), {@link #remove remove()}, and {@link #clear}; note, these methods must verify
 * the key {@link #isVisible isVisible()} before making any changes.
 *
 * <p>
 * Additional subclass notes:
 * <ul>
 *  <li>{@link #iterator} returns an {@link java.util.Iterator} whose {@link java.util.Iterator#remove Iterator.remove()}
 *      method delegates to this instance's {@link #remove remove()} method.
 * </ul>
 *
 * <p><b>Prefix Mode</b></p>
 *
 * <p>
 * Instances support a "prefix mode" where the {@code byte[]} keys may have arbitrary trailing garbage, which is ignored,
 * and so by definition no key can be a prefix of any other key. The length of the prefix is determined implicitly by the
 * number of bytes produced by {@link #encode encode()} or consumed by {@link #decode decode()}.
 * When not in prefix mode, {@link #decode decode()} <b>must</b> consume the entire key to preserve correct semantics.
 *
 * <p><b>Key Restrictions</b></p>
 *
 * <p>
 * Instances are configured with an (optional) {@link KeyRange}; when {@linkplain #bounds range restriction} is in
 * effect, this key range corresponds to the bounds.
 *
 * <p>
 * Instances also support filtering visible values using a {@link KeyFilter}; see {@link #filterKeys filterKeys()}.
 * To be {@linkplain #isVisible} in the set, keys must both be in the {@link KeyRange} and pass the {@link KeyFilter}.
 *
 * <p><b>Concurrent Modifications</b></p>
 *
 * <p>
 * This implementation never throws {@link java.util.ConcurrentModificationException}; instead, iterators always
 * see the most up-to-date state of the associated {@link KVStore}.
 *
 * @see AbstractKVNavigableMap
 * @param <E> element type
 */
public abstract class AbstractKVNavigableSet<E> extends AbstractNavigableSet<E> {

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
     * Key filter, or null if all keys in the key range should be visible.
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
    protected AbstractKVNavigableSet(KVStore kv, boolean prefixMode) {
        this(kv, prefixMode, (KeyRange)null);
    }

    /**
     * Convenience constructor for when the range of visible {@link KVStore} keys is all keys sharing a given prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix prefix defining minimum and maximum keys
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code prefix} is null or empty
     */
    protected AbstractKVNavigableSet(KVStore kv, boolean prefixMode, ByteData prefix) {
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
    protected AbstractKVNavigableSet(KVStore kv, boolean prefixMode, KeyRange keyRange) {
        this(kv, prefixMode, false, keyRange, null, new Bounds<>());
    }

    /**
     * Internal constructor. Used for creating sub-sets and reversed views.
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
    protected AbstractKVNavigableSet(KVStore kv, boolean prefixMode, boolean reversed,
      KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
        super(bounds);
        Preconditions.checkArgument(kv != null, "null kv");
        this.kv = kv;
        this.prefixMode = prefixMode;
        this.reversed = reversed;
        this.keyRange = keyRange;
        this.keyFilter = keyFilter;
    }

// NavigableSet

    @Override
    public boolean isEmpty() {
        return this.firstPair() == null;
    }

    @Override
    public E first() {
        final KVPair pair = this.firstPair();
        if (pair == null)
            throw new NoSuchElementException();
        return this.decode(pair.getKey().newReader());
    }

    @Override
    public E last() {
        final KVPair pair = this.lastPair();
        if (pair == null)
            throw new NoSuchElementException();
        return this.decode(pair.getKey().newReader());
    }

    @Override
    public E pollFirst() {
        final KVPair pair = this.firstPair();
        if (pair == null)
            return null;
        final ByteData key = pair.getKey();
        final E elem = this.decode(key.newReader());
        this.kv.remove(key);
        return elem;
    }

    @Override
    public E pollLast() {
        final KVPair pair = this.lastPair();
        if (pair == null)
            return null;
        final ByteData key = pair.getKey();
        final E elem = this.decode(key.newReader());
        this.kv.remove(key);
        return elem;
    }

    private KVPair firstPair() {
        return this.reversed ? this.highestPair() : this.lowestPair();
    }

    private KVPair lastPair() {
        return this.reversed ? this.lowestPair() : this.highestPair();
    }

    private KVPair lowestPair() {
        KVPair pair = null;
        if (this.keyFilter == null) {
            pair = this.keyRange != null ?
              this.kv.getAtLeast(this.keyRange.getMin(), this.keyRange.getMax()) :
              this.kv.getAtLeast(null, null);
        } else {
            final ByteData[] bounds = this.initialBounds();
            if (bounds == null)
                return null;
            while (true) {
                if ((pair = this.kv.getAtLeast(bounds[0], bounds[1])) == null)
                    return null;
                final ByteData key = pair.getKey();
                assert this.keyRange == null || this.keyRange.contains(key);
                if (this.keyFilter.contains(key))
                    break;
                bounds[0] = ByteUtil.getNextKey(key);
                if (!this.seekHigher(bounds))
                    return null;
            }
        }
        return pair;
    }

    private KVPair highestPair() {
        KVPair pair = null;
        if (this.keyFilter == null) {
            pair = this.keyRange != null ?
              this.kv.getAtMost(this.keyRange.getMax(), this.keyRange.getMin()) :
              this.kv.getAtMost(null, null);
        } else {
            final ByteData[] bounds = this.initialBounds();
            if (bounds == null)
                return null;
            while (true) {
                if ((pair = this.kv.getAtMost(bounds[1], bounds[0])) == null)
                    return null;
                final ByteData key = pair.getKey();
                assert this.keyRange == null || this.keyRange.contains(key);
                if (this.keyFilter.contains(key))
                    break;
                bounds[1] = key;
                if (!this.seekLower(bounds))
                    return null;
            }
        }
        return pair;
    }

    // Create bounds that intersect the keyRange (if any) and the keyFilter (which must exist)
    private ByteData[] initialBounds() {
        assert this.keyFilter != null;
        final ByteData[] bounds = this.keyRange != null ?
          new ByteData[] { this.keyRange.getMin(), this.keyRange.getMax() } :
          new ByteData[] { ByteData.empty(), null };
        if (!this.seekHigher(bounds) || !this.seekLower(bounds))
            return null;
        return bounds;
    }

    private boolean seekHigher(ByteData[] bounds) {
        final ByteData higherKey = this.keyFilter.seekHigher(bounds[0]);
        assert higherKey == null || higherKey.compareTo(bounds[0]) >= 0;
        return (bounds[0] = higherKey) != null;
    }

    private boolean seekLower(ByteData[] bounds) {
        final ByteData startKey = bounds[1] != null ? bounds[1] : ByteData.empty();
        final ByteData lowerKey = this.keyFilter.seekLower(startKey);
        assert lowerKey == null || startKey.isEmpty() || lowerKey.compareTo(startKey) <= 0;
        assert lowerKey == null || !lowerKey.isEmpty() || startKey.isEmpty();
        if (lowerKey == null)
            return false;
        bounds[1] = !lowerKey.isEmpty() ? lowerKey : null;
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object obj) {

        // Encode key and check visibility
        final ByteData key = this.encodeVisible(obj, false);
        if (key == null)
            return false;

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
                return false;
            assert pair.getKey().startsWith(key);
            return true;
        } else
            return this.kv.get(key) != null;
    }

    @Override
    public CloseableIterator<E> iterator() {
        return new AbstractKVIterator<E>(this.kv, this.prefixMode, this.reversed, this.keyRange, this.keyFilter) {

            @Override
            protected E decodePair(KVPair pair, ByteData.Reader keyReader) {
                return AbstractKVNavigableSet.this.decode(keyReader);
            }

            @Override
            protected void doRemove(E value, KVPair pair) {
                AbstractKVNavigableSet.this.remove(value);
            }
        };
    }

    /**
     * Create a view of this instance with additional filtering applied to the underlying {@code byte[]} encoded keys.
     * Any set element for which the corresponding key does not pass {@code keyFilter} will be effectively hidden from view.
     *
     * <p>
     * The restrictions of the given {@link KeyFilter} will be added to any current {@link KeyFilter} restrictions on this instance.
     * The {@link #bounds} associated with this instance will not change.
     *
     * @param keyFilter additional key filtering to apply
     * @return filtered view of this instance
     * @throws IllegalArgumentException if {@code keyFilter} is null
     */
    public NavigableSet<E> filterKeys(KeyFilter keyFilter) {
        Preconditions.checkArgument(keyFilter != null, "null keyFilter");
        if (this.keyFilter != null)
            keyFilter = KeyFilterUtil.intersection(keyFilter, this.keyFilter);
        return this.createSubSet(this.reversed, this.keyRange, keyFilter, this.bounds);
    }

    @Override
    protected boolean isWithinLowerBound(E elem) {
        if (!super.isWithinLowerBound(elem))
            return false;
        if (this.keyRange == null)
            return true;
        final ByteData.Writer writer = ByteData.newWriter();
        this.encode(writer, elem);
        return KeyRange.compare(writer.toByteData(), this.keyRange.getMin()) >= 0;
    }

    @Override
    protected boolean isWithinUpperBound(E elem) {
        if (!super.isWithinUpperBound(elem))
            return false;
        if (this.keyRange == null)
            return true;
        final ByteData.Writer writer = ByteData.newWriter();
        this.encode(writer, elem);
        return KeyRange.compare(writer.toByteData(), this.keyRange.getMax()) < 0;
    }

    @Override
    protected final NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds) {

        // Determine the direction of the new sub-set
        final boolean newReversed = this.reversed ^ reverse;

        // Determine new min and max keys
        final KeyRange newKeyRange = this.buildKeyRange(newReversed ? newBounds.reverse() : newBounds);

        // Create subset
        return this.createSubSet(newReversed, newKeyRange, this.keyFilter, newBounds);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds and
     * the given {@link KeyFilter}, if any.
     * The bounds are consistent with the reversed ordering (i.e., reversed if {@code reverse} is true)
     * and have already been range-checked against this instance's bounds.
     *
     * @param newReversed whether the new set's ordering should be reversed (implies {@code newBounds} are also inverted,
     *  but <i>not</i> {@code keyRange}); note: means "absolutely" reversed, not relative to this instance
     * @param newKeyRange new key range, or null for none; will be consistent with {@code bounds}, if any
     * @param newKeyFilter new key filter, or null for none
     * @param newBounds new bounds
     * @return restricted and/or filtered view of this instance
     * @throws IllegalArgumentException if {@code newBounds} is null
     */
    protected abstract NavigableSet<E> createSubSet(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<E> newBounds);

    /**
     * Encode the given object into a {@code byte[]} key.
     * Note that this method must throw {@link IllegalArgumentException}, not {@link ClassCastException}
     * or {@code NullPointerException}, if {@code obj} does not have the correct type or is an illegal null value.
     *
     * @param writer output for encoded {@code byte[]} key corresponding to {@code obj}
     * @param obj set element
     * @throws IllegalArgumentException if {@code obj} is not of the required Java type supported by this set
     * @throws IllegalArgumentException if {@code obj} is null and this set does not support null elements
     */
    protected abstract void encode(ByteData.Writer writer, Object obj);

    /**
     * Decode an element from a {@code byte[]} key.
     *
     * <p>
     * If not in prefix mode, all of {@code reader} must be consumed; otherwise, the consumed portion
     * is the prefix and any following keys with the same prefix are ignored.
     *
     * @param reader input for encoded bytes
     * @return decoded set element
     */
    protected abstract E decode(ByteData.Reader reader);

    /**
     * Determine if the given {@code byte[]} key is visible in this set according to the configured
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
     * Encode the given object, if possible, and verify corresponding {@code byte[]} key is visible,
     * otherwise return null or throw an exception.
     * Delegates to {@link #encode(ByteData.Writer, Object)} to attempt the actual encoding.
     *
     * @param obj object to encode, possibly null
     * @param fail whether, if {@code obj} can't be encoded, to throw an exception (true) or return null (false)
     * @return encoed key for {@code obj}, or null if {@code fail} is false and {@code obj} has the wrong type or is out of bounds
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} has the wrong type
     * @throws IllegalArgumentException if {@code fail} is true and the resulting key is not {@linkplain #isVisible visible}
     */
    protected ByteData encodeVisible(Object obj, boolean fail) {
        final ByteData.Writer writer = ByteData.newWriter();
        try {
            this.encode(writer, obj);
        } catch (IllegalArgumentException e) {
            if (!fail)
                return null;
            throw e;
        }
        final ByteData key = writer.toByteData();
        if (this.keyRange != null && !this.keyRange.contains(key)) {
            if (fail)
                throw new IllegalArgumentException(String.format("value is out of bounds: %s", obj));
            return null;
        }
        if (this.keyFilter != null && !this.keyFilter.contains(key)) {
            if (fail)
                throw new IllegalArgumentException(String.format("value is filtered out: %s", obj));
            return null;
        }
        return key;
    }

    /**
     * Derive a new {@link KeyRange} from (possibly) new element bounds. The given bounds must <i>not</i> ever be reversed.
     */
    private KeyRange buildKeyRange(Bounds<E> bounds) {
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
            this.encode(writer, bounds.getLowerBound());
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
            this.encode(writer, bounds.getUpperBound());
            newMaxKey = writer.toByteData();
            if (bounds.getUpperBoundType().isInclusive())
                newMaxKey = this.prefixMode ? ByteUtil.getKeyAfterPrefix(newMaxKey) : ByteUtil.getNextKey(newMaxKey);
            if (maxKey != null)
                newMaxKey = ByteUtil.min(newMaxKey, maxKey);
            if (minKey != null)
                newMaxKey = ByteUtil.max(newMaxKey, minKey);
            break;
        }
        return new KeyRange(newMinKey != null ? newMinKey : ByteData.empty(), newMaxKey);
    }
}
