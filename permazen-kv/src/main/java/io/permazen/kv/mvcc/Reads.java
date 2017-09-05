
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.util.ByteUtil;

/**
 * Holds a set of reads from a {@link io.permazen.kv.KVStore}.
 *
 * <p>
 * Only the (ranges of) keys read are retained, not the values.
 *
 * <p>
 * Instances are not thread safe.
 */
public class Reads extends KeyRanges {

// Constructors

    /**
     * Construct an empty instance.
     */
    public Reads() {
    }

    /**
     * Construct an instance containing the given key ranges.
     *
     * @param ranges initial key ranges
     * @throws IllegalArgumentException if {@code ranges} is null
     */
    public Reads(KeyRanges ranges) {
        super(ranges);
    }

    /**
     * Constructor to deserialize an instance created by {@link #serialize serialize()}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @throws IOException if an I/O error occurs
     * @throws java.io.EOFException if the input ends unexpectedly
     * @throws IllegalArgumentException if {@code input} is null
     * @throws IllegalArgumentException if {@code input} is invalid
     */
    public Reads(InputStream input) throws IOException {
        super(input);
    }

// MVCC

    /**
     * Determine whether any of the given mutations conflict with any of the keys read by this instance.
     *
     * <p>
     * If this method returns false, then if two transactions T1 and T2 are based on the same underlying
     * {@link io.permazen.kv.KVStore} snapshot, and T1 writes {@code mutations} and T2 reads according to this instance,
     * then T2 can be ordered after T1 while still preserving linearizable semantics. That is, the given {@code mutations}
     * are invisible to this instance.
     *
     * <p>
     * This method guarantees that it will access the given {@code mutations} in this order: removes, puts, adjusts.
     *
     * @param mutations mutations to check for conflicts
     * @return true if the {@code mutations} are invisible to this instance, false if there is a read/write conflict
     * @throws IllegalArgumentException if {@code mutations} is null
     */
    public boolean isConflict(Mutations mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");

        // Check for read/remove conflicts
        for (KeyRange remove : mutations.getRemoveRanges()) {
            if (this.intersects(remove))
                return true;
        }

        // Check for read/write conflicts
        for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs()) {
            if (this.contains(entry.getKey()))
                return true;
        }

        // Check for read/adjust conflicts
        for (Map.Entry<byte[], Long> entry : mutations.getAdjustPairs()) {
            if (this.contains(entry.getKey()))
                return true;                    // read/adjust conflict
        }

        // No conflicts
        return false;
    }

    /**
     * List the conflicts between the given mutations and any of the keys read by this instance.
     *
     * <p>
     * This gives descriptive details about the conflicts reported by {@link #isConflict isConflict()};
     * see that method for information on these conflicts. This method returns an empty list if and only
     * if {@link #isConflict isConflict()} returns false.
     *
     * @param mutations mutations to check for conflicts with this instance
     * @return a description of each conflict between this instance and the given mutations
     * @throws IllegalArgumentException if {@code mutations} is null
     */
    public List<String> getConflicts(Mutations mutations) {

        // Sanity check
        Preconditions.checkArgument(mutations != null, "null mutations");

        // Prepare list
        final ArrayList<String> conflicts = new ArrayList<>();

        // Check removes
        for (KeyRange remove : mutations.getRemoveRanges()) {
            if (this.intersects(remove)) {
                final KeyRanges intersection = new KeyRanges(remove);
                intersection.intersect(this);
                final StringBuilder buf = new StringBuilder();
                for (KeyRange range : intersection) {
                    if (buf.length() > 0)
                        buf.append(", ");
                    buf.append(range.isSingleKey() ? ByteUtil.toString(range.getMin()) : range);
                }
                conflicts.add("read/remove conflict: " + buf);
            }
        }

        // Check puts
        for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs()) {
            if (this.contains(entry.getKey()))
                conflicts.add("read/write conflict: " + ByteUtil.toString(entry.getKey()));
        }

        // Check adjusts
        for (Map.Entry<byte[], Long> entry : mutations.getAdjustPairs()) {
            if (this.contains(entry.getKey()))
                conflicts.add("read/adjust conflict: " + ByteUtil.toString(entry.getKey()));
        }

        // Return conflicts
        return conflicts;
    }

// Cloneable

    @Override
    public Reads clone() {
        return (Reads)super.clone();
    }

    @Override
    public Reads immutableSnapshot() {
        return (Reads)super.immutableSnapshot();
    }
}

