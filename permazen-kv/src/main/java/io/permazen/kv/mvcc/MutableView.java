
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a mutable view of an underlying, read-only {@link KVStore}.
 *
 * <p>
 * Instances intercept all operations to the underlying {@link KVStore}, recording mutations in a {@link Writes} instance
 * instead of applying them to the {@link KVStore}. Instances then provide a view of the mutated {@link KVStore} based those
 * mutations. Mutations that overwrite previous mutations are consolidated.
 *
 * <p>
 * Unlike writes, reads are passed through to the underlying {@link KVStore}, except where they intersect a previous write.
 *
 * <p>
 * In all cases, the underlying {@link KVStore} is never modified.
 *
 * <p>
 * Instances ensure that counter adjustment mutations never overlap put or remove mutations.
 *
 * <p>
 * <b>Read Tracking</b>
 *
 * <p>
 * During construction, instances may be configured to record all keys read into a {@link Reads} object (this is typically
 * used for MVCC conflict detection). When reads are being tracked, tracking may be temporarily suspended in the current
 * thread only via {@link #withoutReadTracking withoutReadTracking()}. Read tracking may be permanently disabled (and any
 * recorded reads discarded) via {@link #disableReadTracking}.
 *
 * <p>
 * Instances are thread safe; however, directly accessing the associated {@link Reads} or {@link Writes} is not thread safe
 * without first locking the containing instance.
 */
@ThreadSafe
public class MutableView extends AbstractKVStore implements Cloneable {

    private static final ThreadLocal<Boolean> WITHOUT_READ_TRACKING = new ThreadLocal<>();  // boolean value is "allowWrites"

    @GuardedBy("this")
    private KVStore kv;
    @GuardedBy("this")
    private /*final*/ Writes writes;
    @GuardedBy("this")
    private Reads reads;
    @GuardedBy("this")
    private boolean readOnly;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * The instance will use a new, empty {@link Reads} instance for read tracking.
     *
     * @param kv underlying {@link KVStore}
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public MutableView(KVStore kv) {
        this(kv, new Reads(), new Writes());
    }

    /**
     * Constructor with optional read tracking.
     *
     * @param kv underlying {@link KVStore}
     * @param trackReads true to enable read tracking, or false for none
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public MutableView(KVStore kv, boolean trackReads) {
        this(kv, trackReads ? new Reads() : null, new Writes());
    }

    /**
     * Constructor with no read tracking and caller-provided {@link Writes}.
     *
     * @param kv underlying {@link KVStore}
     * @param writes recorded writes
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code writes} is null
     */
    public MutableView(KVStore kv, Writes writes) {
        this(kv, null, writes);
    }

    /**
     * Constructor using caller-provided {@link Reads} (optional) and {@link Writes}.
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
    }

// Public methods

    /**
     * Get the underlying {@link KVStore} associated with this instance.
     *
     * @return underlying {@link KVStore}
     */
    public synchronized KVStore getKVStore() {
        return this.kv;
    }

    /**
     * Swap out the underlying {@link KVStore} associated with this instance.
     *
     * <p>
     * Note: the new {@link KVStore} should have a consistent encoding of counter values as the previous {@link KVStore},
     * otherwise a concurrent thread may read previously written counter values back incorrectly.
     *
     * @param kv new underlying {@link KVStore}
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public synchronized void setKVStore(KVStore kv) {
        Preconditions.checkArgument(kv != null, "null kv");
        this.kv = kv;
    }

    /**
     * Get the {@link Reads} associated with this instance.
     *
     * <p>
     * This includes all keys explicitly or implicitly read by calls to
     * {@link #get get()}, {@link #getAtLeast getAtLeast()}, {@link #getAtMost getAtMost()}, and {@link #getRange getRange()}.
     *
     * <p>
     * The returned object is "live" and should only be accessed while synchronized on this instance.
     *
     * @return reads recorded, or null if this instance is not configured to record reads or read tracking has
     *  been permanently disabled via {@link #disableReadTracking}
     */
    public synchronized Reads getReads() {
        return this.reads;
    }

    /**
     * Get the {@link Writes} associated with this instance.
     *
     * <p>
     * The returned object should only be accessed while synchronized on this instance.
     *
     * @return writes recorded
     */
    public synchronized Writes getWrites() {
        return this.writes;
    }

// ReadTracking

    /**
     * Permanently disable read tracking and discard the {@link Reads} associated with this instance.
     *
     * <p>
     * Can be used to save some memory when read tracking information is no longer needed.
     */
    public synchronized void disableReadTracking() {
        this.reads = null;
    }

    /**
     * Temporarily disable read tracking in the current thread only while performing the given action.
     *
     * <p>
     * If {@code allowWrites} is false, then write attempts will generate an {@link IllegalStateException}.
     *
     * @param allowWrites whether to allow writes
     * @param action the action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void withoutReadTracking(boolean allowWrites, Runnable action) {
        Preconditions.checkArgument(action != null, "null action");
        final Boolean prevAllowWrites = WITHOUT_READ_TRACKING.get();
        WITHOUT_READ_TRACKING.set(allowWrites);
        try {
            action.run();
        } finally {
            WITHOUT_READ_TRACKING.set(prevAllowWrites);
        }
    }

    private static boolean isAllowWrites() {
        final Boolean allowWrites = WITHOUT_READ_TRACKING.get();
        return allowWrites == null || allowWrites;
    }

    /**
     * Configure this instance as read-only.
     *
     * <p>
     * Any subsequent invocations of {@link #put put()}, {@link #remove remove()}, {@link #removeRange removeRange()},
     * or {@link #adjustCounter adjustCounter()} will result in an {@link IllegalStateException}.
     */
    public synchronized void setReadOnly() {
        this.readOnly = true;
    }

// KVStore

    @Override
    public synchronized byte[] get(byte[] key) {

        // Check puts
        byte[] value = this.writes.getPuts().get(key);
        if (value != null)
            return this.applyCounterAdjustment(key, value).clone();

        // Check removes
        if (this.writes.getRemoves().contains(key))
            return null;                                            // we can ignore adjustments of missing values

        // Read from underlying k/v store
        value = this.kv.get(key);

        // Record the read
        this.recordReads(key, ByteUtil.getNextKey(key));

        // Apply counter adjustments
        if (value != null)                                          // we can ignore adjustments of missing values
            value = this.applyCounterAdjustment(key, value).clone();

        // Done
        return value;
    }

    @Override
    public synchronized CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        return new RangeIterator(minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        Preconditions.checkState(!this.readOnly, "instance is read-only");
        Preconditions.checkState(MutableView.isAllowWrites(), "writes disallowed while read tracking is disabled");

        // Overwrite any counter adjustment
        this.writes.getAdjusts().remove(key);

        // Record the put
        this.writes.getPuts().put(key.clone(), value.clone());
    }

    @Override
    public synchronized void remove(byte[] key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkState(!this.readOnly, "instance is read-only");
        Preconditions.checkState(MutableView.isAllowWrites(), "writes disallowed while read tracking is disabled");

        // Overwrite any counter adjustment
        this.writes.getAdjusts().remove(key);

        // Overwrite any put
        this.writes.getPuts().remove(key);

        // Record the remove
        this.writes.getRemoves().add(new KeyRange(key));
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {

        // Sanity check
        Preconditions.checkState(!this.readOnly, "instance is read-only");
        Preconditions.checkState(MutableView.isAllowWrites(), "writes disallowed while read tracking is disabled");

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
        this.writes.getRemoves().add(new KeyRange(minKey, maxKey));
    }

    @Override
    public byte[] encodeCounter(long value) {
        final KVStore currentKV;
        synchronized (this) {
            currentKV = this.kv;
        }
        return currentKV.encodeCounter(value);
    }

    @Override
    public long decodeCounter(byte[] bytes) {
        final KVStore currentKV;
        synchronized (this) {
            currentKV = this.kv;
        }
        return currentKV.decodeCounter(bytes);
    }

    @Override
    public synchronized void adjustCounter(byte[] key, long amount) {

        // Sanity check
        Preconditions.checkState(!this.readOnly, "instance is read-only");
        Preconditions.checkState(MutableView.isAllowWrites(), "writes disallowed while read tracking is disabled");

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
        if (oldAdjust != null)
            amount += oldAdjust;

        // Record/update adjustment
        if (amount != 0)
            this.writes.getAdjusts().put(key, amount);
        else if (oldAdjust != null)
            this.writes.getAdjusts().remove(key);
    }

    @Override
    public synchronized void apply(Mutations mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        super.apply(mutations);
    }

// Cloneable

    /**
     * Clone this instance.
     *
     * <p>
     * The clone will have the same underlying {@link KVStore}, but its own {@link Reads} and {@link Writes},
     * which will themselves be cloned from this instance's copies.
     *
     * @return clone of this instance
     */
    @Override
    public synchronized MutableView clone() {
        final MutableView clone;
        try {
            clone = (MutableView)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        if (this.reads != null)
            clone.reads = this.reads.clone();
        clone.writes = this.writes.clone();
        return clone;
    }

// Object

    @Override
    public synchronized String toString() {
        return this.getClass().getSimpleName()
          + "[writes=" + this.writes
          + (this.reads != null ? ",reads=" + this.reads : "")
          + (this.readOnly ? ",r/o" : "")
          + "]";
    }

// Internal methods

    // Apply accumulated counter adjustments to the value, if any. If no adjustment necessary, returns same "value" object.
    private synchronized byte[] applyCounterAdjustment(byte[] key, byte[] value) {

        // Is there an adjustment of this key?
        assert key != null;
        final Long adjust = this.writes.getAdjusts().get(key);
        if (adjust == null || adjust == 0)
            return value;

        // Decode value we just read as a counter
        final long counterValue;
        try {
            counterValue = this.kv.decodeCounter(value);
        } catch (IllegalArgumentException e) {
            return value;                                       // previous adjustment was bogus because value was not decodable
        }

        // Adjust counter value by accumulated adjustment value and re-encode
        final byte[] adjustedValue = this.kv.encodeCounter(counterValue + adjust);
        assert adjustedValue != null;
        return adjustedValue;
    }

    // Record that keys were read in the range [minKey, maxKey)
    // This method must be invoked while continuously synchronized with the read
    private void recordReads(byte[] minKey, byte[] maxKey) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert minKey != null;
        assert maxKey == null || ByteUtil.compare(minKey, maxKey) < 0;

        // Not tracking reads?
        if (this.reads == null || WITHOUT_READ_TRACKING.get() != null)
            return;

        // Define the range
        KeyRange range = new KeyRange(minKey, maxKey);

        // If read is entirely contained in a remove range, it did not really go through to k/v store
        if (this.writes.getRemoves().contains(range))
            return;

        // If there is a put at the beginning of the range, that first key did not really go through to k/v store
        if (this.writes.getPuts().containsKey(minKey)) {
            if ((range = new KeyRange(ByteUtil.getNextKey(minKey), maxKey)).isEmpty())
                return;
        }

        // Add range
        this.reads.add(range);
    }

// RangeIterator

    @ThreadSafe
    private class RangeIterator implements CloseableIterator<KVPair> {

        // Locking order: (1) RangeIterator (2) MutableView

        private final boolean reverse;          // iteration direction
        private final byte[] limit;             // limit of iteration; exclusive if forward, inclusive if reverse

        @GuardedBy("this")
        private KVStore kv;                     // underlying k/v store corresponding to this.kviter
        @GuardedBy("this")
        private byte[] cursor;                  // current position; inclusive if forward, exclusive if reverse
        @GuardedBy("this")
        private KVPair next;                    // the next k/v pair queued up, or null if not found yet
        @GuardedBy("this")
        private byte[] removeKey;               // key to remove if remove() is invoked
        @GuardedBy("this")
        private boolean finished;

        // Position in underlying k/v store
        @GuardedBy("this")
        private CloseableIterator<KVPair> kviter;   // k/v store iterator, if any left
        @GuardedBy("this")
        private KVPair kvnext;                  // next kvstore pair, if already retrieved

        // Position in puts
        @GuardedBy("this")
        private KVPair putnext;                 // next put pair, if already retrieved
        @GuardedBy("this")
        private boolean putdone;                // no more pairs left in puts

        RangeIterator(byte[] minKey, byte[] maxKey, boolean reverse) {
            assert Thread.holdsLock(MutableView.this);

            // Realize minKey
            if (minKey == null)
                minKey = ByteUtil.EMPTY;

            // Initialize cursor
            this.kv = MutableView.this.kv;
            this.kviter = this.kv.getRange(minKey, maxKey, reverse);
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
            Preconditions.checkState(this.removeKey != null);
            MutableView.this.remove(this.removeKey);
            this.removeKey = null;
        }

        private synchronized boolean findNext() {

            // Invariants & checks
            assert this.next == null;
            assert this.cursor != null || this.reverse;
            assert this.limit != null || !this.reverse;
            assert this.kviter != null || this.kvnext == null;

            // Exhausted?
            if (this.finished)
                return false;

            // Keep track of starting range of keys read from the underlying k/v store
            byte[] readStart;

            // Find the next underlying k/v pair, if we don't already have it. Whenever we access the underlying KVStore
            // we synchronize on this MutableView; this prevents it from changing out from under us while we're using it,
            // as well as avoiding races with other threads doing put(), remove(), etc.
            synchronized (MutableView.this) {

                // Detect if the underlying key/value store has been swapped out; if so, we must get a new iterator
                if (this.kviter != null && this.kv != MutableView.this.kv) {
                    this.closeKVStoreIterator();
                    this.kv = MutableView.this.kv;
                    this.kviter = this.reverse ?
                      this.kv.getRange(this.limit, this.cursor, true) :
                      this.kv.getRange(this.cursor, this.limit, false);
                }

                // Advance to the next key/value pair
                readStart = this.cursor;
                if (this.kviter != null && this.kvnext == null) {

                    // Get removes
                    final KeyRanges removes = MutableView.this.writes.getRemoves();

                    // Find next key/value pair that has not been removed
                    while (true) {

                        // Get next k/v pair in underlying key/value store, if any
                        if (!this.kviter.hasNext()) {
                            this.closeKVStoreIterator();
                            break;
                        }
                        this.kvnext = this.kviter.next();
                        assert this.kvnext != null;
                        assert !this.isPastLimit(this.kvnext.getKey()) :
                          "key " + ByteUtil.toString(this.kvnext.getKey())
                          + " is past limit " + ByteUtil.toString(this.limit);
                        assert this.isPast(this.kvnext.getKey(), this.cursor) :
                          "key " + ByteUtil.toString(this.kvnext.getKey())
                          + " is not past cursor " + ByteUtil.toString(this.cursor);

                        // If k/v pair has been removed, skip past the matching remove range
                        final KeyRange[] ranges = removes.findKey(this.kvnext.getKey());
                        if (ranges[0] == ranges[1] && ranges[0] != null) {
                            final KeyRange removeRange = ranges[0];

                            // If the removed range contains the starting cursor as well, we can shrink our recorded read range
                            final byte[] removeRangeEnd = this.reverse ? removeRange.getMin() : removeRange.getMax();
                            if (this.reverse) {
                                final byte[] removeRangeStart = removeRange.getMax();
                                if (readStart != null
                                  && (removeRangeStart == null || ByteUtil.compare(readStart, removeRangeStart) <= 0))
                                    readStart = removeRangeEnd;
                            } else if (removeRange.contains(readStart))
                                readStart = removeRangeEnd;

                            // Find the end of the remove range (if any)
                            if (removeRangeEnd == null
                             || this.isPastLimit(removeRangeEnd)
                             || (this.reverse && Arrays.equals(removeRangeEnd, this.limit))) {
                                this.closeKVStoreIterator();
                                break;
                            }

                            // Skip over it and restart iterator
                            this.closeKVStoreIterator();
                            final byte[] iterMin;
                            final byte[] iterMax;
                            if (this.reverse) {
                                iterMin = this.limit;
                                iterMax = removeRangeEnd;
                            } else {
                                iterMin = removeRangeEnd;
                                iterMax = this.limit;
                            }
                            this.kviter = MutableView.this.kv.getRange(iterMin, iterMax, this.reverse);
                            continue;
                        }

                        // Got one
                        break;
                    }
                }

                // Find next put pair, if we don't already have it
                if (!this.putdone && this.putnext == null) {
                    final Map.Entry<byte[], byte[]> putEntry;
                    if (this.reverse) {
                        putEntry = this.cursor != null ?
                          MutableView.this.writes.getPuts().lowerEntry(this.cursor) :
                          MutableView.this.writes.getPuts().lastEntry();
                    } else
                        putEntry = MutableView.this.writes.getPuts().ceilingEntry(this.cursor);
                    if (putEntry == null || this.isPastLimit(putEntry.getKey())) {
                        this.putnext = null;
                        this.putdone = true;
                    } else
                        this.putnext = new KVPair(putEntry.getKey().clone(), putEntry.getValue().clone());
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
                    final int diff = this.reverse ?
                      ByteUtil.compare(this.kvnext.getKey(), this.putnext.getKey()) :
                      ByteUtil.compare(this.putnext.getKey(), this.kvnext.getKey());
                    if (diff <= 0) {
                        this.next = this.putnext;
                        this.putnext = null;
                        if (diff == 0)
                            this.kvnext = null;                     // the kvstore key was overridden by the put key
                    } else {
                        this.next = this.kvnext;
                        this.kvnext = null;
                    }
                }

                // Record that we read from everything we just scanned over in the underlying KVStore
                final byte[] skipMin;
                final byte[] skipMax;
                if (this.reverse) {
                    skipMin = this.next != null ? this.next.getKey() : this.limit;
                    skipMax = readStart;
                } else {
                    skipMin = readStart;
                    skipMax = this.next != null ? ByteUtil.getNextKey(this.next.getKey()) : this.limit;
                }
                if (skipMin != null && (skipMax == null || ByteUtil.compare(skipMin, skipMax) < 0))
                    MutableView.this.recordReads(skipMin, skipMax);
            }

            // Finished?
            if (this.next == null) {
                this.finished = true;
                return false;
            }

            // Apply any counter adjustment to the retrieved value, if appropriate
            final byte[] adjustedValue = MutableView.this.applyCounterAdjustment(this.next.getKey(), this.next.getValue());
            if (adjustedValue != this.next.getValue())
                this.next = new KVPair(this.next.getKey(), adjustedValue);

            // Update cursor
            this.cursor = this.reverse ? this.next.getKey() : ByteUtil.getNextKey(this.next.getKey());

            // Done
            return true;
        }

        private boolean isPastLimit(byte[] key) {
            return this.isPast(key, this.limit);
        }

        private boolean isPast(byte[] key, byte[] mark) {
            return this.reverse ?
              mark == null || ByteUtil.compare(key, mark) < 0 :
              mark != null && ByteUtil.compare(key, mark) >= 0;
        }

        private void closeKVStoreIterator() {
            assert Thread.holdsLock(this);
            if (this.kviter != null) {
                this.kviter.close();
                this.kviter = null;
            }
            this.kvnext = null;
        }

    // Closeable

        @Override
        public synchronized void close() {
            this.closeKVStoreIterator();
            this.putdone = true;
        }
    }
}
