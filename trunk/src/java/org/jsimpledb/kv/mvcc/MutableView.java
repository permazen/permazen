
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import com.google.common.base.Converter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.ConvertedNavigableSet;

/**
 * Provides a mutable view of an underlying, read-only {@link KVStore}.
 *
 * <p>
 * Instances intercept all operations to the underlying {@link KVStore}, recording mutations internally instead of applying
 * them to the {@link KVStore}. Instances then provide a view of the mutated {@link KVStore} based those recorded accesses.
 * Mutations are recorded as put, remove, and counter adjust operations. Mutations that overwrite previous mutations
 * are consolidated.
 * </p>
 *
 * <p>
 * Unlike writes, read accesses are passed through to the {@link KVStore}, except where they intersect a previous write.
 * Read accesses may also be optionally recorded. Null and non-null reads are tracked separately; however, this distinction
 * assumes that reads from the underlying {@link KVStore} are repeatable.
 * </p>
 *
 * <p>
 * In all cases, then underlying {@link KVStore} is never modified.
 * </p>
 *
 * <p>
 * At any time, the set of accumulated mutations may be applied to a given target {@link KVStore} via {@link #applyTo applyTo()}.
 * </p>
 *
 * <p>
 * Instances are also capable of performing certain MVCC-related calculations, such as whether two transactions may be re-ordered.
 * </p>
 *
 * <p>
 * Instances are thread safe.
 * </p>
 */
public class MutableView extends AbstractKVStore implements KVStore {

    private final KVStore kv;

    // Mutations
    private final TreeMap<byte[], byte[]> puts = new TreeMap<>(ByteUtil.COMPARATOR);
    private KeyRanges removes = KeyRanges.EMPTY;
    private final TreeMap<byte[], Long> adjusts = new TreeMap<>(ByteUtil.COMPARATOR);

    // Reads (optional)
    private TreeSet<byte[]> liveReads;
    private KeyRanges deadReads;

// Constructors

    /**
     * Constructor.
     *
     * @param kv underlying {@link KVStore}
     * @param recordReads whether to also record reads
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public MutableView(KVStore kv, boolean recordReads) {
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        this.kv = kv;
        if (recordReads) {
            this.liveReads = new TreeSet<byte[]>(ByteUtil.COMPARATOR);
            this.deadReads = KeyRanges.EMPTY;
        }
        synchronized (this) { }
    }

// Public methods

    /**
     * Get the set of keys read by this instance that returned a non-null value.
     *
     * <p>
     * This includes all keys explicitly read with non-null values by calls to
     * {@link #get get()}, {@link #getAtLeast getAtLeast()}, {@link #getAtMost getAtMost()}, and {@link #getRange getRange()}.
     *
     * @return unmodifiable set of live keys read
     * @throws UnsupportedOperationException if this instance is not configured to record reads
     */
    public synchronized NavigableSet<byte[]> getLiveReads() {
        if (this.liveReads == null)
            throw new UnsupportedOperationException("this instance is not configured to record reads");
        return Sets.unmodifiableNavigableSet(this.liveReads);
    }

    /**
     * Get the set of keys read by this instance that returned a null value.
     *
     * <p>
     * This includes all keys explicitly read or implicitly seek'ed over by calls to
     * {@link #get get()}, {@link #getAtLeast getAtLeast()}, {@link #getAtMost getAtMost()}, and {@link #getRange getRange()}.
     *
     * @return null keys read
     * @throws UnsupportedOperationException if this instance is not configured to record reads
     */
    public synchronized KeyRanges getDeadReads() {
        if (this.deadReads == null)
            throw new UnsupportedOperationException("this instance is not configured to record reads");
        return this.deadReads;
    }

    /**
     * Disable read tracking and discard all read tracking information from this instance.
     *
     * <p>
     * Can be used to save some memory when read tracking information is no longer needed.
     */
    public synchronized void disableReadTracking() {
        this.liveReads = null;
        this.deadReads = null;
    }

    /**
     * Get the set of {@link #put put()}s recorded by this instance.
     *
     * <p>
     * The caller must not modify any of the {@code byte[]} arrays in the returned map.
     * </p>
     *
     * @return unmodifiable mapping from key to corresponding value put
     */
    public synchronized NavigableMap<byte[], byte[]> getPuts() {
        return Maps.unmodifiableNavigableMap(this.puts);
    }

    /**
     * Get the set of keys removed by this instance.
     *
     * <p>
     * This includes all keys removed by {@link #remove remove()}, {@link #removeRange removeRange()},
     * or {@link Iterator#remove} invoked on the {@link Iterator} returned by {@link #getRange getRange()}.
     *
     * @return keys removed
     */
    public synchronized KeyRanges getRemoves() {
        return this.removes;
    }

    /**
     * Get the set of counter keys and values {@linkplain #adjustCounter adjusted} by this instance.
     *
     * <p>
     * The caller must not modify any of the {@code byte[]} arrays in the returned map.
     * </p>
     *
     * @return unmodifiable mapping from key to corresponding counter adjustment
     */
    public synchronized NavigableMap<byte[], Long> getAdjusts() {
        return Maps.unmodifiableNavigableMap(this.adjusts);
    }

    /**
     * Apply all mutating operations recorded by this instance to the given {@link KVStore}.
     *
     * @param target target for recorded mutations
     * @throws IllegalArgumentException if {@code target} is null
     */
    public synchronized void applyTo(KVStore target) {
        if (target == null)
            throw new IllegalArgumentException("null target");
        assert this.check();
        for (KeyRange remove : this.removes.asList())
            target.removeRange(remove.getMin(), remove.getMax());
        for (Map.Entry<byte[], byte[]> entry : this.puts.entrySet())
            target.put(entry.getKey(), entry.getValue());
        for (Map.Entry<byte[], Long> entry : this.adjusts.entrySet())
            target.adjustCounter(entry.getKey(), entry.getValue());
    }

    /**
     * Determine whether any of the mutations peformed by {@code that} affect any of the keys read by this instance.
     * This instance must have read tracking enabled.
     *
     * <p>
     * If this method returns true, then if this instance and {@code that} both have the same underlying {@link KVStore} snapshot,
     * then applying {@code that}'s mutations followed by this instance's mutations preserves linearizable semantics.
     * That is, then end result is the same as if this instance were using the result of {@code that}'s mutations
     * as its underlying {@link KVStore} (instead of the original snapshot).
     *
     * @param that other instance
     * @return true if {@code that} affects keys read by this instance, otherwise false
     * @throws IllegalStateException if this instance does not have read tracking enabled
     * @throws IllegalArgumentException if {@code that} is null
     */
    public boolean isAffectedBy(MutableView that) {

        // Sanity check
        if (that == null)
            throw new IllegalArgumentException("null that");
        if (this.liveReads == null)
            throw new IllegalStateException("read tracking not enabled");

        // Snapshot 'that' state
        final TreeSet<byte[]> thatPuts;
        final List<KeyRange> thatRemoves;
        final TreeSet<byte[]> thatAdjusts;
        synchronized (that) {
            thatPuts = new TreeSet<byte[]>(that.puts.navigableKeySet());
            thatRemoves = that.removes.asList();
            thatAdjusts = new TreeSet<byte[]>(that.adjusts.navigableKeySet());
        }

        // Compare states
        synchronized (this) {

            // Look for live read conflicts with (a) any put, (b) any remove, (c) any adjustment
            int removeIndex = 0;
            int removeLimit = thatRemoves.size();
            for (byte[] key : this.liveReads) {

                // Check puts
                if (thatPuts.contains(key))
                    return true;                    // read/put conflict

                // Check removes
                while (removeIndex < removeLimit) {
                    final int diff = thatRemoves.get(removeIndex).compareTo(key);
                    if (diff == 0)
                        return true;                // read/remove conflict
                    if (diff > 0)
                        break;
                    removeIndex++;
                }

                // Check adjustments
                if (thatAdjusts.contains(key))
                    return true;                    // read/adjust conflict
            }

            // Look for dead read conflicts with (a) any put, (b) any adjustment
            for (KeyRange range : this.deadReads.asList()) {
                final byte[] min = range.getMin();
                final byte[] max = range.getMax();

                // Check puts
                final NavigableSet<byte[]> putRange = max != null ?
                  thatPuts.subSet(min, true, max, false) : thatPuts.tailSet(min, true);
                if (!putRange.isEmpty())
                    return true;                    // read/write conflict

                // Check adjustments
                final NavigableSet<byte[]> adjustRange = max != null ?
                  thatAdjusts.subSet(min, true, max, false) : thatAdjusts.tailSet(min, true);
                if (!adjustRange.isEmpty())
                    return true;                    // read/write conflict
            }
        }

        // No conflict!
        return false;
    }

// KVStore

    @Override
    public synchronized byte[] get(byte[] key) {

        // Sanity check
        assert this.check();

        // Check puts
        final byte[] putValue = this.puts.get(key);
        if (putValue != null)
            return putValue;

        // Check removes
        if (this.removes.contains(key))
            return null;

        // Read from k/v store
        byte[] value = this.kv.get(key);

        // Record the read
        if (value != null)
            this.recordLiveRead(key);
        else
            this.recordDeadRead(key);

        // Check counter adjustments
        if (value == null)                      // we can ignore adjustments of missing values
            return null;
        final Long adjust = this.adjusts.get(key);
        if (adjust != null) {

            // Decode value we just read as a counter
            final long counterValue;
            try {
                counterValue = this.kv.decodeCounter(value);
            } catch (IllegalArgumentException e) {
                this.adjusts.remove(key);       // previous adjustment was bogus because value was not decodable
                return value;
            }

            // Adjust counter value by adjustment and re-encode
            value = this.kv.encodeCounter(counterValue + adjust);
        }

        // Done
        return value;
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {

        // Sanity check
        assert this.check();

        // Build iterator
        return new RangeIterator(minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {

        // Sanity check
        assert this.check();
        if (key == null)
            throw new IllegalArgumentException("null key");
        if (value == null)
            throw new IllegalArgumentException("null value");

        // Overwrite any counter adjustment
        this.adjusts.remove(key);

        // Overwrite any removal
        if (this.removes.contains(key))
            this.removes = this.removes.remove(new KeyRange(key));

        // Record the put
        this.puts.put(key.clone(), value.clone());
    }

    @Override
    public synchronized void remove(byte[] key) {

        // Sanity check
        assert this.check();
        if (key == null)
            throw new IllegalArgumentException("null key");

        // Overwrite any counter adjustment
        this.adjusts.remove(key);

        // Overwrite any put
        this.puts.remove(key);

        // Record the remove
        this.removes = this.removes.add(new KeyRange(key));
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {

        // Sanity check
        assert this.check();

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Overwrite any puts and counter adjustments
        if (maxKey != null) {
            this.puts.subMap(minKey, maxKey).clear();
            this.adjusts.subMap(minKey, maxKey).clear();
        } else {
            this.puts.tailMap(minKey).clear();
            this.adjusts.tailMap(minKey).clear();
        }

        // Record the remove
        this.removes = this.removes.add(new KeyRange(minKey, maxKey));
    }

    @Override
    public byte[] encodeCounter(long value) {
        return this.kv.encodeCounter(value);
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        return this.kv.decodeCounter(bytes);
    }

    @Override
    public synchronized void adjustCounter(byte[] key, long amount) {

        // Sanity check
        assert this.check();

        // Check puts
        final byte[] putValue = this.puts.get(key);
        if (putValue != null) {
            final long value;
            try {
                value = this.kv.decodeCounter(putValue);
            } catch (IllegalArgumentException e) {
                return;                                 // previously put value was not decodable, so ignore this adjustment
            }
            this.puts.put(key, this.kv.encodeCounter(value + amount));
            return;
        }

        // Check removes
        if (this.removes.contains(key))
            return;

        // Calculate new, cumulative adjustment
        Long oldAdjust = this.adjusts.get(key);
        final long adjust = (oldAdjust != null ? oldAdjust : 0) + amount;

        // Record/update adjustment
        if (adjust != 0)
            this.adjusts.put(key, adjust);
        else
            this.adjusts.remove(key);
    }

// Object

    @Override
    public String toString() {
        final Converter<String, byte[]> byteConverter = ByteUtil.STRING_CONVERTER.reverse();
        final ConvertedNavigableMap<String, String, byte[], byte[]> putsView
          = new ConvertedNavigableMap<>(this.puts, byteConverter, byteConverter);
        final ConvertedNavigableMap<String, Long, byte[], Long> adjustsView
          = new ConvertedNavigableMap<>(this.adjusts, byteConverter, Converter.<Long>identity());
        final ConvertedNavigableSet<String, byte[]> liveReadsView
          = this.liveReads != null ? new ConvertedNavigableSet<>(this.liveReads, byteConverter) : null;
        return this.getClass().getSimpleName()
          + "[puts=" + putsView
          + (!this.removes.isEmpty() ? ",dels=" + this.removes : "")
          + (!this.adjusts.isEmpty() ? ",adjusts=" + adjustsView : "")
          + (liveReadsView != null ? ",liveReads=" + liveReadsView : "")
          + (this.deadReads != null ? ",deadReads=" + this.deadReads : "")
          + "]";
    }

// Internal methods

    // Record that a non-null value was read from the key
    private synchronized void recordLiveRead(byte[] key) {

        // Not tracking reads or already tracked?
        if (this.liveReads == null || this.liveReads.contains(key))
            return;

        // Check if read did not really go through to k/v store
        if (this.puts.containsKey(key) || this.removes.contains(key))
            return;

        // Record read
        this.liveReads.add(key);
    }

    // Record that a null value was read from the key
    private synchronized void recordDeadRead(byte[] key) {
        this.recordDeadReads(key, ByteUtil.getNextKey(key));
    }

    // Record that null values were read from the range [minKey, maxKey)
    private synchronized void recordDeadReads(byte[] minKey, byte[] maxKey) {

        // Not tracking reads?
        if (this.deadReads == null)
            return;

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Already tracked?
        final KeyRange readRange = new KeyRange(minKey, maxKey);
        if (this.deadReads.contains(readRange))
            return;

        // Subtract out the part of the read range that did not really go through to k/v store due to puts or removes
        KeyRanges readRanges = new KeyRanges(readRange);
        final Set<byte[]> putKeys = (maxKey != null ? this.puts.subMap(minKey, maxKey) : this.puts.tailMap(minKey)).keySet();
        for (byte[] key : putKeys)
            readRanges = readRanges.remove(new KeyRange(key));
        readRanges = readRanges.intersection(this.removes.inverse());

        // Record reads
        if (!readRanges.isEmpty())
            this.deadReads = this.deadReads.union(readRanges);
    }

// Debugging

    // Verify puts, removes, and adjusts are all mutually disjoint
    private synchronized boolean check() {
        this.verifyDisjoint(this.getPutRanges(), this.removes, this.getAdjustRanges());
        return true;
    }

    private synchronized void verifyDisjoint(KeyRanges... rangeses) {
        for (int i = 0; i < rangeses.length - 1; i++) {
            for (int j = i + 1; j < rangeses.length; j++)
                assert rangeses[i].intersection(rangeses[j]).isEmpty();
        }
    }

    private synchronized KeyRanges getPutRanges() {
        KeyRanges ranges = KeyRanges.EMPTY;
        for (byte[] key : this.puts.keySet())
            ranges = ranges.add(new KeyRange(key));
        return ranges;
    }

    private synchronized KeyRanges getAdjustRanges() {
        KeyRanges ranges = KeyRanges.EMPTY;
        for (byte[] key : this.adjusts.keySet())
            ranges = ranges.add(new KeyRange(key));
        return ranges;
    }

// RangeIterator

    private class RangeIterator implements Iterator<KVPair> {

        private final KVPairIterator pi;
        private final byte[] limit;
        private final boolean reverse;

        private byte[] cursor;                  // current position; inclusive if forward, exclusive if reverse
        private KVPair next;                    // the next k/v pair queued up, or null if not found yet
        private byte[] removeKey;               // key to remove if remove() is invoked
        private boolean finished;

        RangeIterator(byte[] minKey, byte[] maxKey, boolean reverse) {

            // Realize minKey
            if (minKey == null)
                minKey = ByteUtil.EMPTY;

            // Build KVPairIterator that omits keys we've put or removed so far; this is safe even if more keys are put and/or
            // removed after creation, because the set "keys we've put or removed so far" can only increase over time.
            synchronized (MutableView.this) {
                final KeyRanges putsAndRemoves = MutableView.this.getPutRanges().union(MutableView.this.removes);
                this.pi = new KVPairIterator(MutableView.this.kv, new KeyRange(minKey, maxKey), putsAndRemoves.inverse(), reverse);
            }

            // Initialize cursor
            this.cursor = reverse ? maxKey : minKey;
            this.limit = reverse ? minKey : maxKey;
            this.reverse = reverse;
        }

        @Override
        public synchronized boolean hasNext() {
            return this.next != null || this.findNext();
        }

        @Override
        public synchronized KVPair next() {
            if (this.next == null && !this.findNext())
                throw new NoSuchElementException();
            final KVPair pair = this.next;
            assert pair != null;
            this.removeKey = pair.getKey();
            this.next = null;
            return pair;
        }

        @Override
        public synchronized void remove() {
            if (this.removeKey == null)
                throw new IllegalStateException();
            MutableView.this.remove(this.removeKey);
            this.removeKey = null;
        }

        private synchronized boolean findNext() {

            // Invariants
            assert this.next == null;
            assert this.cursor != null || this.reverse;

            // Exhausted?
            if (this.finished)
                return false;

            // Compare against most current MutableView puts and removes
            synchronized (MutableView.this) {

                // Find the next k/v pair not filtered out by (newly added) removes, if any
                KVPair readPair = null;
                while (this.pi.hasNext()) {
                    readPair = this.pi.next();
                    if (MutableView.this.removes.contains(readPair.getKey())) {
                        readPair = null;
                        continue;
                    }
                    break;
                }

                // Find the next put, if any
                final Map.Entry<byte[], byte[]> putEntry = this.reverse ?
                  (this.cursor != null ? MutableView.this.puts.lowerEntry(this.cursor) : MutableView.this.puts.lastEntry()) :
                  MutableView.this.puts.ceilingEntry(this.cursor);
                final KVPair putPair = putEntry != null ? new KVPair(putEntry) : null;

                // Figure out which pair wins (read or put)
                final KVPair pair;
                int diff = 0;
                if (readPair == null && putPair == null)
                    pair = null;
                else if (readPair == null)
                    pair = putPair;
                else if (putPair == null)
                    pair = readPair;
                else {
                    diff = ByteUtil.compare(putPair.getKey(), readPair.getKey());
                    if (reverse)
                        diff = -diff;
                    pair = diff <= 0 ? putPair : readPair;                  // if there's a tie, the put wins
                }

                // Record that we read a live value from the underlying KVStore
                if (pair != null && pair != putPair)
                    MutableView.this.recordLiveRead(pair.getKey());

                // Record that we read nulls from everything we skipped over in the underlying KVStore
                final byte[] skipMin;
                final byte[] skipMax;
                if (this.reverse) {
                    skipMin = pair != null ? ByteUtil.getNextKey(pair.getKey()) : ByteUtil.EMPTY;
                    skipMax = this.cursor;
                } else {
                    skipMin = this.cursor;
                    skipMax = pair != null ? pair.getKey() : null;
                }
                if (skipMax == null || ByteUtil.compare(skipMin, skipMax) < 0)
                    MutableView.this.recordDeadReads(skipMin, skipMax);

                // Finished?
                if (pair == null) {
                    this.finished = true;
                    return false;
                }

                // Update state
                this.next = pair;
                this.cursor = pair != null ? (this.reverse ? pair.getKey() : ByteUtil.getNextKey(pair.getKey())) : null;

                // If a put appeared prior to the KVPPairIterator's next k/v pair, backup the KVPairIterator for next time
                if (diff < 0)
                    this.pi.setNextTarget(this.cursor);

                // Done
                return this.next != null;
            }
        }
    }
}

