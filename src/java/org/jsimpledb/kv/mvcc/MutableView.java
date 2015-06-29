
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.KVPair;
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
 * Reads may also be optionally recorded.
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
    private final Writes writes;
    private Reads reads;

// Constructors

    /**
     * Constructor.
     *
     * @param kv underlying {@link KVStore}
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public MutableView(KVStore kv) {
        this(kv, new Reads(), new Writes());
    }

    /**
     * Constructor using caller-provided {@link Reads} (optional} and {@link Writes}.
     *
     * @param kv underlying {@link KVStore}
     * @param reads recorded reads, or null for none
     * @param writes recorded writes
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code writes} is null
     */
    public MutableView(KVStore kv, Reads reads, Writes writes) {
        Preconditions.checkArgument(kv != null, "null kv");
        Preconditions.checkArgument(writes != null, "null writes");
        this.kv = kv;
        this.reads = reads;
        this.writes = writes;
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

        // Check puts
        byte[] value = this.writes.getPuts().get(key);
        if (value != null)
            return this.applyCounterAdjustment(key, value);

        // Check removes
        if (this.writes.getRemoves().contains(key))
            return null;

        // Read from underlying k/v store
        value = this.kv.get(key);

        // Record the read
        this.recordReads(key, ByteUtil.getNextKey(key));

        // Apply counter adjustments
        if (value != null)                                          // we can ignore adjustments of missing values
            value = this.applyCounterAdjustment(key, value);

        // Done
        return value;
    }

    @Override
    public Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {

        // Build iterator
        return new RangeIterator(minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");

        // Overwrite any counter adjustment
        this.writes.getAdjusts().remove(key);

        // Record the put
        this.writes.getPuts().put(key.clone(), value.clone());
    }

    @Override
    public synchronized void remove(byte[] key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Overwrite any counter adjustment
        this.writes.getAdjusts().remove(key);

        // Overwrite any put
        this.writes.getPuts().remove(key);

        // Record the remove
        this.writes.setRemoves(this.writes.getRemoves().add(new KeyRange(key)));
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {

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
        final Long oldAdjust = this.writes.getAdjusts().get(key);
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

    // Apply accumulated counter adjustments to the value, if any
    private synchronized byte[] applyCounterAdjustment(byte[] key, byte[] value) {

        // Is there an adjustment of this key?
        assert key != null;
        final Long adjust = this.writes.getAdjusts().get(key);
        if (adjust == null)
            return value;

        // Decode value we just read as a counter
        final long counterValue;
        try {
            counterValue = this.kv.decodeCounter(value);
        } catch (IllegalArgumentException e) {
            this.writes.getAdjusts().remove(key);       // previous adjustment was bogus because value was not decodable
            return value;
        }

        // Adjust counter value by accumulated adjustment value and re-encode
        return this.kv.encodeCounter(counterValue + adjust);
    }

    // Record that keys were read in the range [minKey, maxKey)
    private synchronized void recordReads(byte[] minKey, byte[] maxKey) {

        // Not tracking reads?
        if (this.reads == null)
            return;

        // Realize minKey
        if (minKey == null)
            minKey = ByteUtil.EMPTY;

        // Already tracked?
        final KeyRange readRange = new KeyRange(minKey, maxKey);
        if (this.reads.getReads().contains(readRange))
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
            this.reads.setReads(this.reads.getReads().union(readRanges));
    }

// RangeIterator

    private class RangeIterator implements Iterator<KVPair> {

        private final byte[] limit;
        private final boolean reverse;

        private byte[] cursor;                  // current position; inclusive if forward, exclusive if reverse
        private KVPair next;                    // the next k/v pair queued up, or null if not found yet
        private byte[] removeKey;               // key to remove if remove() is invoked
        private boolean finished;

        // Position in underlying k/v store
        private byte[] kvcursor;                // current position in underlying k/v store
        private KVPair kvnext;                  // next kvstore pair, if already retrieved
        private boolean kvdone;                 // no more pairs left in kvstore

        // Position in puts
        private byte[] putcursor;               // current position in puts
        private KVPair putnext;                 // next put pair, if already retrieved
        private boolean putdone;                // no more pairs left in puts

        RangeIterator(byte[] minKey, byte[] maxKey, boolean reverse) {

            // Realize minKey
            if (minKey == null)
                minKey = ByteUtil.EMPTY;

            // Initialize cursors
            this.cursor = reverse ? maxKey : minKey;
            this.kvcursor = this.cursor;
            this.putcursor = this.cursor;
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
            Preconditions.checkState(this.removeKey != null);
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

            // Find the next underlying k/v pair, if we don't already have it
            final KeyRanges removes;
            synchronized (MutableView.this) {
                removes = MutableView.this.writes.getRemoves();
            }
            if (!this.kvdone && this.kvnext == null) {
                while (true) {

                    // Get next k/v pair in underlying key/value store, if any
                    if (this.reverse) {
                        if ((this.kvnext = MutableView.this.kv.getAtMost(this.kvcursor)) == null
                          || ByteUtil.compare(this.kvnext.getKey(), this.limit) < 0) {
                            this.kvnext = null;
                            this.kvdone = true;
                            break;
                        }
                    } else {
                        if ((this.kvnext = MutableView.this.kv.getAtLeast(this.kvcursor)) == null
                          || (this.limit != null && ByteUtil.compare(this.kvnext.getKey(), this.limit) >= 0)) {
                            this.kvnext = null;
                            this.kvdone = true;
                            break;
                        }
                    }
                    assert this.kvnext != null;

                    // If key has been removed, skip past the matching remove range
                    final KeyRange[] ranges = removes.findKey(this.kvnext.getKey());
                    if (ranges[0] == ranges[1] && ranges[0] != null) {
                        if ((this.kvcursor = this.reverse ? ranges[0].getMin() : ranges[0].getMax()) == null) {
                            this.kvnext = null;
                            this.kvdone = true;
                            break;
                        }
                        continue;
                    }

                    // Got one
                    break;
                }
            }

            // Find next put pair, if we don't already have it
            if (!this.putdone && this.putnext == null) {
                Map.Entry<byte[], byte[]> putEntry;
                if (this.reverse) {
                    synchronized (MutableView.this) {
                        putEntry = this.cursor != null ?
                          MutableView.this.writes.getPuts().lowerEntry(this.putcursor) :
                          MutableView.this.writes.getPuts().lastEntry();
                    }
                    if (putEntry == null || ByteUtil.compare(putEntry.getKey(), this.limit) < 0) {
                        putEntry = null;
                        this.putdone = true;
                    }
                } else {
                    synchronized (MutableView.this) {
                        putEntry = MutableView.this.writes.getPuts().ceilingEntry(this.putcursor);
                    }
                    if (putEntry == null || this.limit != null && ByteUtil.compare(putEntry.getKey(), this.limit) >= 0) {
                        putEntry = null;
                        this.putdone = true;
                    }
                }
                if (putEntry != null) {
                    this.putnext = new KVPair(putEntry);
                    this.putcursor = putEntry.getKey();
                } else {
                    this.putdone = true;
                    this.putnext = null;
                }
            }

            // Figure out which pair appears first (k/v or put); if there's a tie, the put wins
            if (this.kvnext == null && this.putnext == null)
                this.next = null;
            else if (this.kvnext == null) {
                this.next = this.putnext;
                this.putnext = null;
            } else if (this.putnext == null) {
                this.next = this.kvnext;
                this.kvnext = null;
            } else {
                int diff = ByteUtil.compare(this.putnext.getKey(), this.kvnext.getKey());
                if (reverse)
                    diff = -diff;
                if (diff < 0) {
                    this.next = this.putnext;
                    this.putnext = null;
                } else if (diff > 0) {
                    this.next = this.kvnext;
                    this.kvnext = null;
                } else {                                    // in a tie, the put wins
                    this.next = this.putnext;
                    this.putnext = null;
                    this.kvnext = null;
                }
            }

            // Record that we read from everything we just scanned over in the underlying KVStore
            final byte[] skipMin;
            final byte[] skipMax;
            if (this.reverse) {
                skipMin = this.next != null ? this.next.getKey() : ByteUtil.EMPTY;
                skipMax = this.cursor;
            } else {
                skipMin = this.cursor;
                skipMax = this.next != null ? ByteUtil.getNextKey(this.next.getKey()) : null;
            }
            if (skipMax == null || ByteUtil.compare(skipMin, skipMax) < 0)
                MutableView.this.recordReads(skipMin, skipMax);

            // Finished?
            if (this.next == null) {
                this.finished = true;
                return false;
            }

            // Apply any counter adjustment to the retrieved value, if appropriate
            final byte[] adjustedValue = MutableView.this.applyCounterAdjustment(this.next.getKey(), this.next.getValue());
            if (adjustedValue != this.next.getValue())
                this.next = new KVPair(this.next.getKey(), adjustedValue);

            // Update cursors
            this.cursor = this.reverse ? this.next.getKey() : ByteUtil.getNextKey(this.next.getKey());
            if (!this.kvdone
              && (this.kvcursor == null || (this.reverse ?
               ByteUtil.compare(this.cursor, this.kvcursor) < 0 :
               ByteUtil.compare(this.cursor, this.kvcursor) > 0)))
                this.kvcursor = this.cursor;
            if (!this.putdone
              && (this.putcursor == null || (this.reverse ?
               ByteUtil.compare(this.cursor, this.putcursor) < 0 :
               ByteUtil.compare(this.cursor, this.putcursor) > 0)))
                this.putcursor = this.cursor;

            // Done
            return true;
        }
    }
}

