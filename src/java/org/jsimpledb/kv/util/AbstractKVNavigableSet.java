
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Iterator;
import java.util.NavigableSet;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.util.AbstractNavigableSet;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.slf4j.LoggerFactory;

/**
 * {@link java.util.NavigableSet} support superclass for sets backed by elements encoded as {@code byte[]}
 * keys in a {@link KVStore} and whose sort order is consistent with their {@code byte[]} key encoding.
 *
 * <p>
 * Instances are configured with (optional) minimum and maximum keys; when {@linkplain #bounds range restriction} is in
 * effect, these minimum and maximum keys must correspond to the bounds. Subclasses must implement the
 * {@linkplain #encode(ByteWriter, Object) encode()} and {@linkplain #decode decode()} methods to convert elements
 * to/from {@code byte[]} keys (associated values are ignored), and
 * {@link #createSubSet(boolean, byte[], byte[], Bounds) createSubSet()} to allow creating reversed and restricted range sub-sets.
 * </p>
 *
 * <p>
 * Subclasses must also implement {@link #comparator comparator()}, and the resulting sort order must be consistent with
 * the sort order of the encoded {@code byte[]} keys (possibly {@link #reversed}).
 * </p>
 *
 * <p>
 * Instances support "prefix mode" where the {@code byte[]} keys may have arbitrary trailing garbage, which is ignored,
 * and so by definition no key can be a prefix of any other key. The length of the prefix is determined implicitly by the
 * number of bytes produced by {@link #encode encode()} or consumed by {@link #decode decode()}.
 * When <b>not</b> in prefix mode, {@link #decode decode()} must consume the entire key (an error is logged if not).
 * </p>
 *
 * <p>
 * This class provides a read-only implementation; for a mutable implementation, subclasses should also implement
 * {@link #add add()} (if appropriate), {@link #remove remove()}, and {@link #clear}; note, these methods must be aware
 * of any {@linkplain AbstractNavigableSet#bounds range restrictions}.
 * </p>
 *
 * <p>
 * Notes on returned collection classes:
 * <ul>
 *  <li>{@link #iterator} returns an {@link Iterator} whose {@link Iterator#remove Iterator.remove()} method delegates
 *      to this instance's {@link #remove remove()} method.
 * </ul>
 * </p>
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
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param kv underlying {@link KVStore}
     */
    public AbstractKVNavigableSet(KVStore kv, boolean prefixMode) {
        this(kv, prefixMode, null, null);
    }

    /**
     * Convenience constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param prefix prefix defining minimum and maximum keys
     * @throws NullPointerException if {@code prefix} is null
     */
    public AbstractKVNavigableSet(KVStore kv, boolean prefixMode, byte[] prefix) {
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
     */
    public AbstractKVNavigableSet(KVStore kv, boolean prefixMode, byte[] minKey, byte[] maxKey) {
        this(kv, prefixMode, false, minKey, maxKey, new Bounds<E>());
    }

    /**
     * Internal constructor. Used for creating sub-sets and reversed views.
     *
     * <p>
     * Note: if upper and/or lower bounds exist, the {@code minKey} and {@code maxKey} must correspond to them exactly.
     * </p>
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
    protected AbstractKVNavigableSet(KVStore kv, boolean prefixMode,
      boolean reversed, byte[] minKey, byte[] maxKey, Bounds<E> bounds) {
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
    @SuppressWarnings("unchecked")
    public boolean contains(Object obj) {

        // Encode key
        final byte[] key = this.encode(obj, false);
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
    public java.util.Iterator<E> iterator() {
        return new Iterator();
    }

    /**
     * Create a subset of this instance that is restricted by {@code byte[]} keys instead of set elements.
     * The {@link #bounds} will not change, but all elements outside of the specified key range will effectively disappear.
     *
     * @param newMinKey new minimum key (inclusive), or null to not change the minimum key
     * @param newMaxKey new maximum key (exclusive), or null to not change the maximum key
     * @throws IllegalArgumentException if {@code newMinKey} is less than the current {@link #minKey}
     * @throws IllegalArgumentException if {@code newMaxKey} is greater than the current {@link #maxKey}
     * @throws IllegalArgumentException if {@code newMinKey > newMmaxKey}
     */
    public NavigableSet<E> keyedSubSet(byte[] newMinKey, byte[] newMaxKey) {
        if (newMinKey == null)
            newMinKey = this.minKey;
        if (newMaxKey == null)
            newMaxKey = this.maxKey;
        if (newMinKey != null && newMaxKey != null && ByteUtil.compare(newMinKey, newMaxKey) > 0)
            throw new IllegalArgumentException("newMinKey > newMaxKey");
        if (newMinKey != null && ByteUtil.compare(newMinKey, this.minKey) < 0)
            throw new IllegalArgumentException("newMinKey < minKey");
        if (newMaxKey != null && ByteUtil.compare(newMaxKey, this.maxKey) > 0)
            throw new IllegalArgumentException("newMaxKey > maxKey");
        return this.createSubSet(this.reversed, newMinKey, newMaxKey, this.bounds);
    }

    @Override
    protected boolean isWithinLowerBound(E elem) {
        if (!super.isWithinLowerBound(elem))
            return false;
        if (this.minKey == null)
            return true;
        final ByteWriter writer = new ByteWriter();
        this.encode(writer, elem);
        return ByteUtil.compare(writer.getBytes(), this.minKey) >= 0;
    }

    @Override
    protected boolean isWithinUpperBound(E elem) {
        if (!super.isWithinUpperBound(elem))
            return false;
        if (this.maxKey == null)
            return true;
        final ByteWriter writer = new ByteWriter();
        this.encode(writer, elem);
        return ByteUtil.compare(writer.getBytes(), this.maxKey) < 0;
    }

    @Override
    protected final NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds) {

        // Determine the direction of the new sub-set
        final boolean newReversed = this.reversed ^ reverse;

        // Determine new min and max keys
        final byte[][] newMinMax = this.buildMinMax(newReversed ? newBounds.reverse() : newBounds);

        // Create subset
        return this.createSubSet(newReversed, newMinMax[0], newMinMax[1], newBounds);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds.
     * The bounds are consistent with the reversed ordering (i.e., reversed if {@code reverse} is true)
     * and have already been range-checked against this instance's bounds.
     *
     * @param newReversed whether the new set's ordering should be reversed (implies {@code newBounds} are also inverted,
     *  but <i>not</i> {@code newMinKey} and {@code newMaxKey}); note: means "absolutely" reversed, not relative to this instance
     * @param newMinKey new minimum visible key (inclusive), or null for none; corresponds to {@code bounds}, if any
     * @param newMaxKey new maximum visible key (exclusive), or null for none; corresponds to {@code bounds}, if any
     * @param newBounds new bounds
     * @throws IllegalArgumentException if {@code newBounds} is null
     */
    protected abstract NavigableSet<E> createSubSet(boolean newReversed, byte[] newMinKey, byte[] newMaxKey, Bounds<E> newBounds);

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
     * @param reader input for encoded bytes
     * @return decoded set element
     */
    protected abstract E decode(ByteReader reader);

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
     * Encode the given object, if possible, otherwise return null or throw an exception.
     *
     * @param obj object to encode, possibly null
     * @param fail whether, if {@code obj} can't be encoded, to throw an exception (true) or return null (false)
     * @return encoed key for {@code obj}, or null if {@code fail} is false and {@code obj} has the wrong type or is out of bounds
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} has the wrong type
     * @throws IllegalArgumentException if {@code fail} is true and {@code obj} is out of bounds
     */
    protected byte[] encode(Object obj, boolean fail) {
        final ByteWriter writer = new ByteWriter();
        try {
            this.encode(writer, obj);
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
    private byte[][] buildMinMax(Bounds<E> bounds) {
        final byte[][] result = new byte[2][];
        switch (bounds.getLowerBoundType()) {
        case NONE:
            result[0] = this.minKey;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encode(writer, bounds.getLowerBound());
            result[0] = writer.getBytes();
            if (!bounds.getLowerBoundType().isInclusive())
                result[0] = this.prefixMode ? ByteUtil.getKeyAfterPrefix(result[0]) : ByteUtil.getNextKey(result[0]);
            result[0] = ByteUtil.min(ByteUtil.max(result[0], this.minKey), this.maxKey);
            break;
        }
        switch (bounds.getUpperBoundType()) {
        case NONE:
            result[1] = this.maxKey;
            break;
        default:
            final ByteWriter writer = new ByteWriter();
            this.encode(writer, bounds.getUpperBound());
            result[1] = writer.getBytes();
            if (bounds.getUpperBoundType().isInclusive())
                result[1] = this.prefixMode ? ByteUtil.getKeyAfterPrefix(result[1]) : ByteUtil.getNextKey(result[1]);
            result[1] = ByteUtil.max(ByteUtil.min(result[1], this.maxKey), this.minKey);
            break;
        }
        return result;
    }

// Iterator

    /**
     * {@link java.util.Iterator} implementation returned by {@link AbstractKVNavigableSet#iterator}.
     */
    private class Iterator implements java.util.Iterator<E> {

        private final java.util.Iterator<KVPair> pairIterator;

        private E removeValue;
        private boolean mayRemove;

        public Iterator() {
            if (AbstractKVNavigableSet.this.prefixMode) {
                this.pairIterator = new KVPairIterator(AbstractKVNavigableSet.this.kv,
                  AbstractKVNavigableSet.this.minKey, AbstractKVNavigableSet.this.maxKey, AbstractKVNavigableSet.this.reversed);
            } else {
                this.pairIterator = AbstractKVNavigableSet.this.kv.getRange(
                  AbstractKVNavigableSet.this.minKey, AbstractKVNavigableSet.this.maxKey, AbstractKVNavigableSet.this.reversed);
            }
        }

        @Override
        public boolean hasNext() {
            return this.pairIterator.hasNext();
        }

        @Override
        public E next() {

            // Get next key/value pair
            final KVPair pair = this.pairIterator.next();

            // Decode key
            final ByteReader reader = new ByteReader(pair.getKey());
            final E value = AbstractKVNavigableSet.this.decode(reader);
            if (!AbstractKVNavigableSet.this.prefixMode && reader.remain() > 0) {
                LoggerFactory.getLogger(this.getClass()).error(AbstractKVNavigableSet.this.getClass().getName()
                  + "@" + Integer.toHexString(System.identityHashCode(AbstractKVNavigableSet.this))
                  + ": " + reader.remain() + " undecoded bytes remain in key " + ByteUtil.toString(pair.getKey()) + " -> " + value);
            }

            // In prefix mode, skip over any additional keys having the same prefix as what we just decoded
            if (AbstractKVNavigableSet.this.prefixMode) {
                final KVPairIterator kvPairIterator = (KVPairIterator)this.pairIterator;
                final byte[] prefix = reader.getBytes(0, reader.getOffset());
                kvPairIterator.setNextTarget(kvPairIterator.isReverse() ? prefix : ByteUtil.getKeyAfterPrefix(prefix));
            }

            // Done
            this.mayRemove = true;
            this.removeValue = value;
            return value;
        }

        @Override
        public void remove() {
            if (!this.mayRemove)
                throw new IllegalStateException();
            AbstractKVNavigableSet.this.remove(this.removeValue);
            this.mayRemove = false;
        }
    }
}

