
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.slf4j.LoggerFactory;

/**
 * {@link Iterator} implementation whose values derive from key/value {@code byte[]} pairs in a {@link KVStore}.
 *
 * <p>
 * Instances are configured with (optional) minimum and maximum keys and support either forward or reverse iteration.
 * Subclasses must implement {@linkplain #decodePair decodePair()} to convert key/value pairs into iteration elements.
 * </p>
 *
 * <p>
 * Instances support "prefix mode" where the {@code byte[]} keys may have arbitrary trailing garbage, which is ignored,
 * and so by definition no key can be a prefix of any other key. The length of the prefix is determined implicitly by the
 * number of key bytes consumed by {@link #decodePair decodePair()}.
 * When <b>not</b> in prefix mode, {@link #decodePair decodePair()} must consume the entire key (an error is logged if not).
 * </p>
 *
 * <p>
 * This class provides a read-only implementation; for a mutable implementation, subclasses should also implement
 * {@link #doRemove doRemove()}.
 * </p>
 *
 * @see AbstractKVNavigableMap
 * @see AbstractKVNavigableSet
 * @param <E> iteration element type
 */
public abstract class AbstractKVIterator<E> implements java.util.Iterator<E> {

    /**
     * The underlying {@link KVStore}.
     */
    protected final KVStore kv;

    /**
     * Whether we are in "prefix" mode.
     */
    protected final boolean prefixMode;

    /**
     * Whether this instance is iterating in the reverse direction (i.e., from {@link #maxKey} down to {@link #minKey}).
     */
    protected final boolean reversed;

    /**
     * Minimum visible key (inclusive), or null for no minimum. Always less than or equal to {@link #maxKey},
     * even if this instance is {@link #reversed}.
     */
    protected final byte[] minKey;

    /**
     * Maximum visible key (exclusive), or null for no maximum. Always greater than or equal to {@link #minKey},
     * even if this instance is {@link #reversed}.
     */
    protected final byte[] maxKey;

    private final Iterator<KVPair> pairIterator;

    private KVPair removePair;
    private E removeValue;

// Constructors

    /**
     * Convenience constructor for when there are no range restrictions.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether to iterate in the reverse direction
     */
    public AbstractKVIterator(KVStore kv, boolean prefixMode, boolean reversed) {
        this(kv, prefixMode, reversed, null, null);
    }

    /**
     * Convenience constructor for when the range of visible {@link KVStore} keys is all keys sharing a given {@code byte[]} prefix.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether to iterate in the reverse direction
     * @param prefix prefix defining minimum and maximum keys
     * @throws NullPointerException if {@code prefix} is null
     */
    public AbstractKVIterator(KVStore kv, boolean prefixMode, boolean reversed, byte[] prefix) {
        this(kv, prefixMode, reversed, prefix, ByteUtil.getKeyAfterPrefix(prefix));
    }

    /**
     * Primary constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param prefixMode whether to allow keys to have trailing garbage
     * @param reversed whether to iterate in the reverse direction
     * @param minKey minimum visible key (inclusive), or null for none
     * @param maxKey maximum visible key (exclusive), or null for none
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code minKey} and {@code maxKey} are both not null but {@code minKey > maxKey}
     */
    public AbstractKVIterator(KVStore kv, boolean prefixMode, boolean reversed, byte[] minKey, byte[] maxKey) {
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) > 0)
            throw new IllegalArgumentException("minKey > maxKey");
        this.kv = kv;
        this.prefixMode = prefixMode;
        this.reversed = reversed;
        this.minKey = minKey != null ? minKey.clone() : null;
        this.maxKey = minKey != null ? maxKey.clone() : null;
        if (this.prefixMode)
            this.pairIterator = new KVPairIterator(this.kv, this.minKey, this.maxKey, this.reversed);
        else
            this.pairIterator = this.kv.getRange(this.minKey, this.maxKey, this.reversed);
    }

// Iterator

    @Override
    public boolean hasNext() {
        return this.pairIterator.hasNext();
    }

    @Override
    public E next() {

        // Get next key/value pair
        final KVPair pair = this.pairIterator.next();

        // Decode key/value pair
        final ByteReader keyReader = new ByteReader(pair.getKey());
        final E value = this.decodePair(pair, keyReader);
        if (!this.prefixMode && keyReader.remain() > 0) {
            LoggerFactory.getLogger(this.getClass()).error(this.getClass().getName() + "@"
              + Integer.toHexString(System.identityHashCode(this)) + ": " + keyReader.remain() + " undecoded bytes remain in key "
              + ByteUtil.toString(pair.getKey()) + ", value " + ByteUtil.toString(pair.getValue()) + " -> " + value);
        }

        // In prefix mode, skip over any additional keys having the same prefix as what we just decoded
        if (this.prefixMode) {
            final KVPairIterator kvPairIterator = (KVPairIterator)this.pairIterator;
            final byte[] prefix = keyReader.getBytes(0, keyReader.getOffset());
            kvPairIterator.setNextTarget(this.reversed ? prefix : ByteUtil.getKeyAfterPrefix(prefix));
        }

        // Done
        this.removePair = pair;
        this.removeValue = value;
        return value;
    }

    @Override
    public void remove() {
        if (this.removePair == null)
            throw new IllegalStateException();
        this.doRemove(this.removeValue, this.removePair);
        this.removePair = null;
    }

// Subclass methods

    /**
     * Decode an iteration element from a key/value pair.
     *
     * <p>
     * If not in prefix mode, all of {@code keyReader} must be consumed; otherwise, the consumed portion
     * is the prefix and any following keys with the same prefix are ignored.
     * </p>
     *
     * @param pair key/value pair
     * @param keyReader key input
     * @return decoded iteration element
     */
    protected abstract E decodePair(KVPair pair, ByteReader keyReader);

    /**
     * Remove the previously iterated value.
     *
     * <p>
     * The implementation in {@link AbstractKVIterator} always throws {@link UnsupportedOperationException}.
     * Subclasses should override to make the iterator mutable.
     * </p>
     *
     * @param value most recent value returned by {@link #next}
     * @param pair the key/value pair corresponding to {@code value}
     */
    protected void doRemove(E value, KVPair pair) {
        throw new UnsupportedOperationException();
    }
}

