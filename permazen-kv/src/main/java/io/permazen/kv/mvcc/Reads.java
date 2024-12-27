
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.util.ByteData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Holds a set of reads from a {@link KVStore}.
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
     * This method is equivalent to invoking {@link #findConflict findConflict()} and checking whether it returns non-null.
     *
     * <p>
     * This method guarantees that it will access the given {@code mutations} in this order: removes, puts, adjusts.
     *
     * @param mutations mutations to check for conflicts
     * @return true if the {@code mutations} are invisible to this instance, false if this instance contains a
     *  key modified by {@code mutations}
     * @throws IllegalArgumentException if {@code mutations} is null
     * @see #findConflict findConflict()
     * @see #getAllConflicts getAllConflicts()
     */
    public boolean isConflict(Mutations mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");

        // Check for read/remove conflicts
        try (Stream<KeyRange> removes = mutations.getRemoveRanges()) {
            if (removes.anyMatch(this::intersects))
                return true;
        }

        // Check for read/write conflicts
        try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
            if (puts.map(Map.Entry::getKey).anyMatch(this::contains))
                return true;
        }

        // Check for read/adjust conflicts
        try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
            if (adjusts.map(Map.Entry::getKey).anyMatch(this::contains))
                return true;
        }

        // No conflicts
        return false;
    }

    /**
     * Determine whether any of the given mutations conflict with any of the keys read by this instance,
     * and report the first conflict found.
     *
     * <p>
     * If this method returns null, then if two transactions T1 and T2 are based on the same underlying
     * {@link KVStore} snapshot, and T1 writes {@code mutations} and T2 reads according to this instance,
     * then T2 can be ordered after T1 while still preserving linearizable semantics. That is, the given {@code mutations}
     * are invisible to this instance.
     *
     * <p>
     * This method guarantees that it will access the given {@code mutations} in this order: removes, puts, adjusts.
     *
     * @param mutations mutations to check for conflicts
     * @return the first {@link Conflict} found, or null if there are no conflicts
     * @throws IllegalArgumentException if {@code mutations} is null
     * @see #getAllConflicts getAllConflicts()
     * @see #isConflict isConflict()
     */
    public Conflict findConflict(Mutations mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        final List<Conflict> conflicts = this.getAllConflicts(mutations, true);
        return !conflicts.isEmpty() ? conflicts.get(0) : null;
    }

    /**
     * List all of the conflicts between the given mutations and any of the keys read by this instance, in {@link String} form.
     *
     * <p>
     * This method simply invokes {@link #getAllConflicts(Mutations) getAllConflicts()} and returns the resulting list
     * converted into {@link String} objects via {@link Conflict#toString}.
     *
     * <p>
     * This method guarantees that it will access the given {@code mutations} in this order: removes, puts, adjusts.
     *
     * @param mutations mutations to check for conflicts with this instance
     * @return a description of each conflict between this instance and the given mutations
     * @throws IllegalArgumentException if {@code mutations} is null
     * @see #getAllConflicts getAllConflicts()
     */
    public List<String> getConflicts(Mutations mutations) {
        final List<Conflict> conflicts = this.getAllConflicts(mutations);
        final ArrayList<String> result = new ArrayList<>(conflicts.size());
        for (Conflict conflict : conflicts)
            result.add(conflict.toString());
        return result;
    }

    /**
     * List all of the conflicts between the given mutations and any of the keys read by this instance.
     *
     * <p>
     * This method guarantees that it will access the given {@code mutations} in this order: removes, puts, adjusts.
     *
     * @param mutations mutations to check for conflicts with this instance
     * @return list of each conflict between this instance and the given mutations
     * @throws IllegalArgumentException if {@code mutations} is null
     * @see #findConflict findConflict()
     * @see #isConflict isConflict()
     */
    public List<Conflict> getAllConflicts(Mutations mutations) {
        return this.getAllConflicts(mutations, false);
    }

    private List<Conflict> getAllConflicts(Mutations mutations, boolean returnFirst) {

        // Sanity check
        Preconditions.checkArgument(mutations != null, "null mutations");

        // Prepare list
        final ArrayList<Conflict> conflictList = new ArrayList<>();

        // Check for read/remove conflicts
        try (Stream<KeyRange> removes = mutations.getRemoveRanges()) {
            for (Iterator<KeyRange> i = removes.iterator(); i.hasNext(); ) {
                final KeyRange remove = i.next();
                if (this.intersects(remove)) {
                    final KeyRanges intersection = new KeyRanges(remove);
                    intersection.intersect(this);
                    for (KeyRange range : intersection) {
                        conflictList.add(new ReadRemoveConflict(range));
                        if (returnFirst)
                            return conflictList;
                    }
                }
            }
        }

        // Check for read/write conflicts
        try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
            for (Iterator<Map.Entry<ByteData, ByteData>> i = puts.iterator(); i.hasNext(); ) {
                final Map.Entry<ByteData, ByteData> entry = i.next();
                final ByteData key = entry.getKey();
                if (this.contains(key)) {
                    conflictList.add(new ReadWriteConflict(key));
                    if (returnFirst)
                        return conflictList;
                }
            }
        }

        // Check for read/adjust conflicts
        try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
            for (Iterator<Map.Entry<ByteData, Long>> i = adjusts.iterator(); i.hasNext(); ) {
                final Map.Entry<ByteData, Long> entry = i.next();
                final ByteData key = entry.getKey();
                if (this.contains(key)) {
                    conflictList.add(new ReadAdjustConflict(key));
                    if (returnFirst)
                        return conflictList;
                }
            }
        }

        // Return conflicts
        return conflictList;
    }

// Cloneable

    @Override
    public Reads clone() {
        return (Reads)super.clone();
    }

    @Override
    public Reads readOnlySnapshot() {
        return (Reads)super.readOnlySnapshot();
    }
}
