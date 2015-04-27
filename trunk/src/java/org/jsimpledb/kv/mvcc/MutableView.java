
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.SizeEstimating;
import org.jsimpledb.util.SizeEstimator;

/**
 * Provides a mutable view of an underlying, read-only {@link KVStore}.
 *
 * <p>
 * Instances intercept all operations to the underlying {@link KVStore}, recording mutations in a {@link Writes} instance
 * instead of applying them to the {@link KVStore}. Instances then provide a view of the mutated {@link KVStore} based those
 * mutations. Mutations that overwrite previous mutations are consolidated.
 * </p>
 *
 * <p>
 * Unlike writes, reads are passed through to the underlying {@link KVStore}, except where they intersect a previous write.
 * Reads may also be optionally recorded. Null ("dead") and non-null ("live") reads are tracked separately (note: reliance on
 * this distinction assumes that reads from the underlying {@link KVStore} are repeatable).
 * </p>
 *
 * <p>
 * In all cases, then underlying {@link KVStore} is never modified.
 * </p>
 *
 * <p>
 * Instances are also capable of performing certain MVCC-related calculations, such as whether two transactions may be re-ordered.
 * </p>
 *
 * <p>
 * Instances are thread safe; however, directly accessing the associated {@link Reads} or {@link Writes} is not thread safe.
 * </p>
 */
public class MutableView extends AbstractKVStore implements SizeEstimating {

    private final KVStore kv;
    private final Writes writes = new Writes();
    private Reads reads;

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
        this.reads = recordReads ? new Reads() : null;
        synchronized (this) { }                                     // because this.reads is not final
    }

// Public methods

    /**
     * Get the {@link Reads} associated with this instance.
     *
     * <p>
     * This includes all keys explicitly or implicitly read by calls to
     * {@link #get get()}, {@link #getAtLeast getAtLeast()}, {@link #getAtMost getAtMost()}, and {@link #getRange getRange()}.
     *
     * @return reads recorded, or null if this instance is not configured to record reads
     */
    public synchronized Reads getReads() {
        return this.reads;
    }

    /**
     * Get the {@link Writes} associated with this instance.
     *
     * @return writes recorded
     */
    public synchronized Writes getWrites() {
        return this.writes;
    }

    /**
     * Disable read tracking and discard the {@link Reads} associated with this instance.
     *
     * <p>
     * Can be used to save some memory when read tracking information is no longer needed.
     */
    public synchronized void disableReadTracking() {
        this.reads = null;
    }

// KVStore

    @Override
    public synchronized byte[] get(byte[] key) {

        // Sanity check
        assert this.check();

        // Check puts
        final byte[] putValue = this.writes.getPuts().get(key);
        if (putValue != null)
            return putValue;

        // Check removes
        if (this.writes.getRemoves().contains(key))
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
        final Long adjust = this.writes.getAdjusts().get(key);
        if (adjust != null) {

            // Decode value we just read as a counter
            final long counterValue;
            try {
                counterValue = this.kv.decodeCounter(value);
            } catch (IllegalArgumentException e) {
                this.writes.getAdjusts().remove(key);       // previous adjustment was bogus because value was not decodable
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
        this.writes.getAdjusts().remove(key);

        // Overwrite any removal
        if (this.writes.getRemoves().contains(key))
            this.writes.setRemoves(this.writes.getRemoves().remove(new KeyRange(key)));

        // Record the put
        this.writes.getPuts().put(key.clone(), value.clone());
    }

    @Override
    public synchronized void remove(byte[] key) {

        // Sanity check
        assert this.check();
        if (key == null)
            throw new IllegalArgumentException("null key");

        // Overwrite any counter adjustment
        this.writes.getAdjusts().remove(key);

        // Overwrite any put
        this.writes.getPuts().remove(key);

        // Record the remove
        this.writes.setRemoves(this.writes.getRemoves().add(new KeyRange(key)));
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
            this.writes.getPuts().subMap(minKey, maxKey).clear();
            this.writes.getAdjusts().subMap(minKey, maxKey).clear();
        } else {
            this.writes.getPuts().tailMap(minKey).clear();
            this.writes.getAdjusts().tailMap(minKey).clear();
        }

        // Record the remove
        this.writes.setRemoves(this.writes.getRemoves().add(new KeyRange(minKey, maxKey)));
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
        final byte[] putValue = this.writes.getPuts().get(key);
        if (putValue != null) {
            final long value;
            try {
                value = this.kv.decodeCounter(putValue);
            } catch (IllegalArgumentException e) {
                return;                                 // previously put value was not decodable, so ignore this adjustment
            }
            this.writes.getPuts().put(key, this.kv.encodeCounter(value + amount));
            return;
        }

        // Check removes
        if (this.writes.getRemoves().contains(key))
            return;

        // Calculate new, cumulative adjustment
        Long oldAdjust = this.writes.getAdjusts().get(key);
        final long adjust = (oldAdjust != null ? oldAdjust : 0) + amount;

        // Record/update adjustment
        if (adjust != 0)
            this.writes.getAdjusts().put(key, adjust);
        else
            this.writes.getAdjusts().remove(key);
    }

// SizeEstimating

    /**
     * Add the estimated size of this instance (in bytes) to the given estimator.
     *
     * <p>
     * The size estimate returned by this method does not include the underlying {@link KVStore}.
     *
     * @param estimator size estimator
     */
    @Override
    public void addTo(SizeEstimator estimator) {
        estimator
          .addObjectOverhead()
          .addReferenceField()                              // kv
          .addField(this.reads)                             // reads
          .addField(this.writes);                           // writes
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[writes=" + this.writes
          + (this.reads != null ? ",reads=" + this.reads : "")
          + "]";
    }

// Internal methods

    // Record that a non-null value was read from the key
    private synchronized void recordLiveRead(byte[] key) {

        // Not tracking reads or already tracked?
        if (this.reads == null || this.reads.getLiveReads().contains(key))
            return;

        // Check if read did not really go through to k/v store
        if (this.writes.getPuts().containsKey(key) || this.writes.getRemoves().contains(key))
            return;

        // Record read
        this.reads.getLiveReads().add(key);
    }

    // Record that a null value was read from the key
    private synchronized void recordDeadRead(byte[] key) {
        this.recordDeadReads(key, ByteUtil.getNextKey(key));
    }

    // Record that null values were read from the range [minKey, maxKey)
    private synchronized void recordDeadReads(byte[] minKey, byte[] maxKey) {

        // Not tracking reads?
        if (this.reads == null)
            return;

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Already tracked?
        final KeyRange readRange = new KeyRange(minKey, maxKey);
        if (this.reads.getDeadReads().contains(readRange))
            return;

        // Subtract out the part of the read range that did not really go through to k/v store due to puts or removes
        KeyRanges readRanges = new KeyRanges(readRange);
        final Set<byte[]> putKeys = (maxKey != null ?
          this.writes.getPuts().subMap(minKey, maxKey) : this.writes.getPuts().tailMap(minKey)).keySet();
        for (byte[] key : putKeys)
            readRanges = readRanges.remove(new KeyRange(key));
        readRanges = readRanges.intersection(this.writes.getRemoves().inverse());

        // Record reads
        if (!readRanges.isEmpty())
            this.reads.setDeadReads(this.reads.getDeadReads().union(readRanges));
    }

// Debugging

    // Verify puts, removes, and adjusts are all mutually disjoint
    private synchronized boolean check() {
        this.verifyDisjoint(this.getPutRanges(), this.writes.getRemoves(), this.getAdjustRanges());
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
        for (byte[] key : this.writes.getPuts().keySet())
            ranges = ranges.add(new KeyRange(key));
        return ranges;
    }

    private synchronized KeyRanges getAdjustRanges() {
        KeyRanges ranges = KeyRanges.EMPTY;
        for (byte[] key : this.writes.getAdjusts().keySet())
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
                final KeyRanges putsAndRemoves = MutableView.this.getPutRanges().union(MutableView.this.writes.getRemoves());
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
                    if (MutableView.this.writes.getRemoves().contains(readPair.getKey())) {
                        readPair = null;
                        continue;
                    }
                    break;
                }

                // Find the next put, if any
                final Map.Entry<byte[], byte[]> putEntry = this.reverse ?
                  (this.cursor != null ?
                   MutableView.this.writes.getPuts().lowerEntry(this.cursor) : MutableView.this.writes.getPuts().lastEntry()) :
                  MutableView.this.writes.getPuts().ceilingEntry(this.cursor);
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

