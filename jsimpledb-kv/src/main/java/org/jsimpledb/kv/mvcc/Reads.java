
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.SizeEstimating;
import org.jsimpledb.util.SizeEstimator;

/**
 * Holds a set of reads from a {@link org.jsimpledb.kv.KVStore}.
 *
 * <p>
 * Only the (ranges of) keys read are retained, not the values.
 *
 * <p>
 * Instances are not thread safe.
 */
public class Reads implements Cloneable, SizeEstimating {

    private KeyRanges reads;

// Constructors

    /**
     * Constructs an empty instance.
     */
    public Reads() {
        this(KeyRanges.EMPTY);
    }

    /**
     * Constructs an instance initialized with the given read ranges
     *
     * @param reads read ranges
     * @throws IllegalArgumentException if {@code reads} is null
     */
    public Reads(KeyRanges reads) {
        this.setReads(reads);
    }

// Public methods

    /**
     * Get the ranges of keys read.
     *
     * @return ranges of keys read
     */
    public KeyRanges getReads() {
        return this.reads;
    }

    /**
     * Set the ranges of keys read.
     *
     * @param reads ranges of keys read
     * @throws IllegalArgumentException if {@code reads} is null
     */
    public void setReads(KeyRanges reads) {
        Preconditions.checkArgument(reads != null, "null reads");
        this.reads = reads;
    }

// MVCC

    /**
     * Determine whether any of the given mutations conflict with any of the keys read by this instance.
     *
     * <p>
     * If this method returns false, then if two transactions T1 and T2 are based on the same underlying
     * {@link org.jsimpledb.kv.KVStore} snapshot, and T1 writes {@code mutations} and T2 reads according to this instance,
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

        // Check removes
        final ArrayList<KeyRange> removes = new ArrayList<KeyRange>();
        for (KeyRange remove : mutations.getRemoveRanges())
            removes.add(remove);
        if (!this.reads.intersection(new KeyRanges(removes)).isEmpty())
            return true;                        // read/remove conflict

        // Check puts
        for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs()) {
            if (this.reads.contains(entry.getKey()))
                return true;                    // read/write conflict
        }

        // Check adjusts
        for (Map.Entry<byte[], Long> entry : mutations.getAdjustPairs()) {
            if (this.reads.contains(entry.getKey()))
                return true;                    // read/adjust conflict
        }

        // No conflicts
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
        this.reads.serialize(out);
    }

    /**
     * Calculate the number of bytes required to serialize this instance via {@link #serialize serialize()}.
     *
     * @return number of serialized bytes
     */
    public long serializedLength() {
        return this.reads.serializedLength();
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
        Preconditions.checkArgument(input != null, "null input");
        return new Reads(KeyRanges.deserialize(input));
    }

// SizeEstimating

    @Override
    public void addTo(SizeEstimator estimator) {
        estimator
          .addObjectOverhead()
          .addField(this.reads);
    }

// Cloneable

    @Override
    public Reads clone() {
        final Reads clone;
        try {
            return (Reads)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

// Object

    @Override
    public String toString() {
        final Converter<String, byte[]> byteConverter = ByteUtil.STRING_CONVERTER.reverse();
        return this.getClass().getSimpleName()
          + "[reads=" + reads
          + "]";
    }
}

