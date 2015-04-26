
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import com.google.common.base.Converter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.util.KeyListEncoder;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableSet;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Holds a set of reads from a {@link org.jsimpledb.kv.KVStore}.
 *
 * <p>
 * Only the keys are retained, not the values.
 * Each read is either a "live" read, meaning a non-null value was returned, or a "dead" read, meaning a null value
 * was returned.
 *
 * <p>
 * Instances are not thread safe.
 * </p>
 */
public class Reads {

    private KeyRanges deadReads = KeyRanges.EMPTY;
    private final TreeSet<byte[]> liveReads = new TreeSet<byte[]>(ByteUtil.COMPARATOR);

// Public methods

    /**
     * Get the keys read where a non-null value was returned.
     *
     * @return set of live keys read
     */
    public NavigableSet<byte[]> getLiveReads() {
        return this.liveReads;
    }

    /**
     * Get the ranges of keys read where a null value was returned.
     *
     * @return ranges of dead keys read
     */
    public KeyRanges getDeadReads() {
        return this.deadReads;
    }

    /**
     * Set the ranges of keys read where a null value was returned.
     *
     * @param deadReads ranges of dead keys read
     * @throws IllegalArgumentException if {@code deadKeys} is null
     */
    public void setDeadReads(KeyRanges deadReads) {
        this.deadReads = deadReads;
    }

// MVCC

    /**
     * Determine whether any of the given {@link Writes} conflict with any of the keys read by this instance.
     *
     * <p>
     * If this method returns false, then if two transactions T1 and T2 are based on the same underlying
     * {@link org.jsimpledb.kv.KVStore} snapshot, and T1 writes {@code writes} and T2 reads according to this instance,
     * then T2 can be ordered after T1 while still preserving linearizable semantics. That is, the given {@code writes}
     * are invisible to this instance.
     *
     * @param writes other instance
     * @return true if the {@code writes} are invisible to this instance, false if there is a read/write conflict
     * @throws IllegalArgumentException if {@code writes} is null
     */
    public boolean isConflict(Writes writes) {

        // Sanity check
        if (writes == null)
            throw new IllegalArgumentException("null writes");

        // Get mutations
        final NavigableSet<byte[]> puts = writes.getPuts().navigableKeySet();
        final List<KeyRange> removes = writes.getRemoves().asList();
        final NavigableSet<byte[]> adjusts = writes.getAdjusts().navigableKeySet();

        // Look for live read conflicts with (a) any put, (b) any remove, (c) any adjustment
        int removeIndex = 0;
        int removeLimit = removes.size();
        for (byte[] key : this.liveReads) {

            // Check puts
            if (puts.contains(key))
                return true;                    // read/put conflict

            // Check removes
            while (removeIndex < removeLimit) {
                final int diff = removes.get(removeIndex).compareTo(key);
                if (diff == 0)
                    return true;                // read/remove conflict
                if (diff > 0)
                    break;
                removeIndex++;
            }

            // Check adjustments
            if (adjusts.contains(key))
                return true;                    // read/adjust conflict
        }

        // Look for dead read conflicts with (a) any put, (b) any adjustment
        for (KeyRange range : this.deadReads.asList()) {
            final byte[] min = range.getMin();
            final byte[] max = range.getMax();

            // Check puts
            final NavigableSet<byte[]> putRange = max != null ?
              puts.subSet(min, true, max, false) : puts.tailSet(min, true);
            if (!putRange.isEmpty())
                return true;                    // read/write conflict

            // Check adjustments
            final NavigableSet<byte[]> adjustRange = max != null ?
              adjusts.subSet(min, true, max, false) : adjusts.tailSet(min, true);
            if (!adjustRange.isEmpty())
                return true;                    // read/write conflict
        }

        // No conflict
        return false;
    }

// Serialization

    /**
     * Serialize this instance.
     *
     * @param out output
     * @throws IOException if an error occurs
     */
    public void serialize(OutputStream out) throws IOException {

        // Live reads
        UnsignedIntEncoder.write(out, liveReads.size());
        byte[] prev = null;
        for (byte[] key : this.liveReads) {
            KeyListEncoder.write(out, key, prev);
            prev = key;
        }

        // Dead reads
        this.deadReads.serialize(out);
    }

    /**
     * Deserialize an instance created by {@link #serialize serialize()}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @return deserialized instance
     * @throws IOException if an I/O error occurs
     * @throws java.io.EOFException if the input ends unexpectedly
     * @throws IllegalArgumentException if malformed input is detected
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static Reads deserialize(InputStream input) throws IOException {

        // Sanity check
        if (input == null)
            throw new IllegalArgumentException("null input");

        // Create instance
        final Reads reads = new Reads();

        // Live reads
        int count = UnsignedIntEncoder.read(input);
        byte[] prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] key = KeyListEncoder.read(input, prev);
            reads.liveReads.add(key);
            prev = key;
        }

        // Dead reads
        reads.deadReads = KeyRanges.deserialize(input);

        // Done
        return reads;
    }

// Object

    @Override
    public String toString() {
        final Converter<String, byte[]> byteConverter = ByteUtil.STRING_CONVERTER.reverse();
        final ConvertedNavigableSet<String, byte[]> liveReadsView = new ConvertedNavigableSet<>(this.liveReads, byteConverter);
        return this.getClass().getSimpleName()
          + "[liveReads=" + liveReadsView
          + ",deadReads=" + deadReads
          + "]";
    }
}

