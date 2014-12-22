
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Iterator;
import java.util.NavigableSet;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.AbstractNavigableSet;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link java.util.NavigableSet} support superclass for sets backed by elements encoded as {@code byte[]}
 * keys in a {@link KVStore} and whose sort order is consistent with their {@code byte[]} key encoding.
 * There must be an equivalence between elements and {@code byte[]} key encodings (i.e., there must be
 * only one valid encoding per set element).
 *
 * <p><b>Subclass Methods</b></p>
 *
 * <p>
 * Subclasses must implement the {@linkplain #encode(ByteWriter, Object) encode()} and {@linkplain #decode decode()}
 * methods to convert elements to/from {@code byte[]} keys (associated values are ignored), and
 * {@link #createSubSet(boolean, KeyRange, KeyFilter, Bounds) createSubSet()}
 * to allow creating reversed and restricted range sub-sets.
 * </p>
 *
 * <p>
 * Subclasses must also implement {@link #comparator comparator()}, and the resulting sort order must be consistent with
 * the sort order of the encoded {@code byte[]} keys (possibly {@link #reversed}).
 * </p>
 *
 * <p>
 * This class provides a read-only implementation; for a mutable implementation, subclasses should also implement
 * {@link #add add()} (if appropriate), {@link #remove remove()}, and {@link #clear}; note, these methods must verify
 * the key {@link #isVisible isVisible()} before making any changes.
 * </p>
 *
 * <p>
 * Additional subclass notes:
 * <ul>
 *  <li>{@link #iterator} returns an {@link Iterator} whose {@link Iterator#remove Iterator.remove()} method delegates
 *      to this instance's {@link #remove remove()} method.
 * </ul>
 * </p>
 *
 * <p><b>Prefix Mode</b></p>
 *
 * <p>
 * Instances support a "prefix mode" where the {@code byte[]} keys may have arbitrary trailing garbage, which is ignored,
 * and so by definition no key can be a prefix of any other key. The length of the prefix is determined implicitly by the
 * number of bytes produced by {@link #encode encode()} or consumed by {@link #decode decode()}.
 * When not in prefix mode, {@link #decode decode()} <b>must</b> consume the entire key to preserve correct semantics.
 * </p>
 *
 * <p><b>Key Restrictions</b></p>
 *
 * <p>
 * Instances are configured with an (optional) {@link KeyRange}; when {@linkplain #bounds range restriction} is in
 * effect, this key range corresponds to the bounds.
 * </p>
 *
 * <p>
 * Instances also support filtering visible values using a {@link KeyFilter}; see {@link #filter filter()}.
 * To be {@linkplain #isVisible} in the set, keys must both be in the {@link KeyRange} and pass the {@link KeyFilter}.
 * </p>
 *
 * <p><b>Concurrent Modifications</b></p>
 *
 * <p>
 * This implementation never throws {@link java.util.ConcurrentModificationException}; instead, iterators always
 * see the most up-to-date state of the associated {@link KVStore}.
 * </p>
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
     * Convenience constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix prefix defining minimum and maximum keys
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code prefix} is null or empty
     */
    protected AbstractKVNavigableSet(KVStore kv, boolean prefixMode, byte[] prefix) {
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
        this(kv, prefixMode, false, keyRange, null, new Bounds<E>());
    }

    /**
     * Internal constructor. Used for creating sub-sets and reversed views.
     *
     * <p>
     * Note: if {@code bounds} are set, then {@code keyRange} must exclude all keys outside of those bounds.
     * </p>
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
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        this.kv = kv;
        this.prefixMode = prefixMode;
        this.reversed = reversed;
        this.keyRange = keyRange;
        this.keyFilter = keyFilter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object obj) {

        // Encode key and check visibility
        final byte[] key = this.encodeVisible(obj, false);
        if (key == null)
            return false;

        // Find key, or some longer key with the same prefix in prefix mode
        final KVPair pair;
        if (this.prefixMode) {
            pair = this.kv.getAtLeast(key);
            return pair != null && ByteUtil.isPrefixOf(key, pair.getKey());
        } else
            return this.kv.get(key) != null;
    }

    @Override
    public Iterator<E> iterator() {
        return new AbstractKVIterator<E>(this.kv, this.prefixMode, this.reversed, this.keyRange, this.keyFilter) {

            @Override
            protected E decodePair(KVPair pair, ByteReader keyReader) {
                return AbstractKVNavigableSet.this.decode(keyReader);
            }

            @Override
            protected void doRemove(E value, KVPair pair) {
                AbstractKVNavigableSet.this.remove(value);
            }
        };
    }

    /**
     * Create a view of this instance with additional filtering applied to the {@code byte[]} encoded values.
     * The restrictions of the given {@link KeyFilter} will be added to any current {@link KeyFilter} restrictions.
     * The {@link #bounds} associated with this instance will not change.
     *
     * @param keyFilter additional key filtering to apply
     * @throws IllegalArgumentException if {@code keyFilter} is null
     */
    public NavigableSet<E> filter(KeyFilter keyFilter) {
        if (keyFilter == null)
            throw new IllegalArgumentException("null keyFilter");
        if (this.keyFilter != null)
            keyFilter = keyFilter.intersection(this.keyFilter);
        return this.createSubSet(this.reversed, this.keyRange, keyFilter, this.bounds);
    }

    @Override
    protected boolean isWithinLowerBound(E elem) {
        if (!super.isWithinLowerBound(elem))
            return false;
        if (this.keyRange == null)
            return true;
        final ByteWriter writer = new ByteWriter();
        this.encode(writer, elem);
        return KeyRange.compare(writer.getBytes(), KeyRange.MIN, this.keyRange.getMin(), KeyRange.MIN) >= 0;
    }

    @Override
    protected boolean isWithinUpperBound(E elem) {
        if (!super.isWithinUpperBound(elem))
            return false;
        if (this.keyRange == null)
            return true;
        final ByteWriter writer = new ByteWriter();
        this.encode(writer, elem);
        return KeyRange.compare(writer.getBytes(), KeyRange.MAX, this.keyRange.getMax(), KeyRange.MAX) < 0;
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
    protected abstract void encode(ByteWriter writer, Object obj);

    /**
     * Decode an element from a {@code byte[]} key.
     *
     * <p>
     * If not in prefix mode, all of {@code reader} must be consumed; otherwise, the consumed portion
     * is the prefix and any following keys with the same prefix are ignored.
     * </p>
     *
     * @param reader input for encoded bytes
     * @return decoded set element
     */
    protected abstract E decode(ByteReader reader);

    /**
     * Determine if the given {@code byte[]} key is visible in this set according to the configured
     * {@link KeyRange} and/or {@link KeyFilter}, if any.
     *
     * @see #filter filter()
     */
    protected boolean isVisible(byte[] key) {
        return (this.keyRange == null || this.keyRange.contains(key))
          && (this.keyFilter == null || this.keyFilter.contains(key));
    }

    /**
     * Encode the given object, if possible, and verify corresponding {@code byte[]} key is visible,
     * otherwise return null or throw an exception.
     * Delegates to {@link #encode(ByteWriter, Object)} to attempt the actual encoding.
     *
     * @param obj object to encode, possibly null
     * @param fail whether, if {@code obj} can't be encoded, to throw an exception (true) or return null (false)
     * @return encoed key for {@code obj}, or null if {@code fail} is false and {@code obj} has the wrong type or is out of bounds
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} has the wrong type
     * @throws IllegalArgumentException if {@code fail} is true and the resulting key is not {@linkplain #isVisible visible}
     */
    protected byte[] encodeVisible(Object obj, boolean fail) {
        final ByteWriter writer = new ByteWriter();
        try {
            this.encode(writer, obj);
        } catch (IllegalArgumentException e) {
            if (!fail)
                return null;
            throw e;
        }
        final byte[] key = writer.getBytes();
        if (this.keyRange != null && !this.keyRange.contains(key)) {
            if (fail)
                throw new IllegalArgumentException("value is out of bounds: " + obj);
            return null;
        }
        if (this.keyFilter != null && !this.keyFilter.contains(key)) {
            if (fail)
                throw new IllegalArgumentException("value is filtered out: " + obj);
            return null;
        }
        return key;
    }

    /**
     * Derive a new {@link KeyRange} from (possibly) new element bounds. The given bounds must <i>not</i> ever be reversed.
     */
    private KeyRange buildKeyRange(Bounds<E> bounds) {
        final byte[] minKey = this.keyRange != null ? this.keyRange.getMin() : null;
        final byte[] maxKey = this.keyRange != null ? this.keyRange.getMax() : null;
        byte[] newMinKey;
        byte[] newMaxKey;
        switch (bounds.getLowerBoundType()) {
        case NONE:
            newMinKey = minKey;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encode(writer, bounds.getLowerBound());
            newMinKey = writer.getBytes();
            if (!bounds.getLowerBoundType().isInclusive())
                newMinKey = this.prefixMode ? ByteUtil.getKeyAfterPrefix(newMinKey) : ByteUtil.getNextKey(newMinKey);
            newMinKey = ByteUtil.min(ByteUtil.max(newMinKey, minKey), maxKey);
            break;
        }
        switch (bounds.getUpperBoundType()) {
        case NONE:
            newMaxKey = maxKey;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encode(writer, bounds.getUpperBound());
            newMaxKey = writer.getBytes();
            if (bounds.getUpperBoundType().isInclusive())
                newMaxKey = this.prefixMode ? ByteUtil.getKeyAfterPrefix(newMaxKey) : ByteUtil.getNextKey(newMaxKey);
            newMaxKey = ByteUtil.max(ByteUtil.min(newMaxKey, maxKey), minKey);
            break;
        }
        return new KeyRange(newMinKey, newMaxKey);
    }
}

