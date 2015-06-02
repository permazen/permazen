
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mvcc;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.util.KeyListEncoder;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.SizeEstimating;
import org.jsimpledb.util.SizeEstimator;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Holds a set of writes to a {@link KVStore}.
 *
 * <p>
 * Each mutation is either a key/value put, the removal of a key range (possibly containing only a single key),
 * or a counter adjustment.
 *
 * <p>
 * Instances are not thread safe.
 * </p>
 */
public class Writes implements Mutations, SizeEstimating {

    private KeyRanges removes = KeyRanges.EMPTY;
    private final TreeMap<byte[], byte[]> puts = new TreeMap<>(ByteUtil.COMPARATOR);
    private final TreeMap<byte[], Long> adjusts = new TreeMap<>(ByteUtil.COMPARATOR);

// Accessors

    /**
     * Get the key ranges removals contained by this instance.
     *
     * @return key ranges removed
     */
    public KeyRanges getRemoves() {
        return this.removes;
    }

    /**
     * Set the key ranges removals contained by this instance.
     *
     * <p>
     * The caller must not modify any of the {@code byte[]} arrays in the returned {@link KeyRanges}.
     * </p>
     *
     * @param removes key ranges removed
     * @throws IllegalArgumentException if {@code removes} is null
     */
    public void setRemoves(KeyRanges removes) {
        if (removes == null)
            throw new IllegalArgumentException("null removes");
        this.removes = removes;
    }

    /**
     * Get the written key/value pairs contained by this instance.
     *
     * <p>
     * The caller must not modify any of the returned {@code byte[]} arrays.
     * </p>
     *
     * @return mutable mapping from key to corresponding value
     */
    public NavigableMap<byte[], byte[]> getPuts() {
        return this.puts;
    }

    /**
     * Get the set of counter adjustments contained by this instance.
     *
     * <p>
     * The caller must not modify any of the returned {@code byte[]} arrays.
     * </p>
     *
     * @return mutable mapping from key to corresponding counter adjustment
     */
    public NavigableMap<byte[], Long> getAdjusts() {
        return this.adjusts;
    }

    /**
     * Determine whether this instance is empty, i.e., contains zero mutations.
     *
     * @return true if this instance contains zero mutations, otherwise false
     */
    public boolean isEmpty() {
        return this.removes.isEmpty() && this.puts.isEmpty() && this.adjusts.isEmpty();
    }

    /**
     * Clear all mutations.
     */
    public void clear() {
        this.removes = KeyRanges.EMPTY;
        this.puts.clear();
        this.adjusts.clear();
    }

// Mutations

    @Override
    public List<KeyRange> getRemoveRanges() {
        return this.removes.asList();
    }

    @Override
    public Iterable<Map.Entry<byte[], byte[]>> getPutPairs() {
        return this.puts.entrySet();
    }

    @Override
    public Iterable<Map.Entry<byte[], Long>> getAdjustPairs() {
        return this.adjusts.entrySet();
    }

// Application

    /**
     * Apply all mutations contained in this instance to the given {@link KVStore}.
     *
     * <p>
     * Mutations are applied in this order: removes, puts, counter adjustments.
     *
     * @param target target for recorded mutations
     * @throws IllegalArgumentException if {@code target} is null
     */
    public void applyTo(KVStore target) {
        Writes.apply(this, target);
    }

    /**
     * Apply all the given {@link Mutations} to the given {@link KVStore}.
     *
     * <p>
     * Mutations are applied in this order: removes, puts, counter adjustments.
     *
     * @param mutations mutations to apply
     * @param target target for mutations
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void apply(Mutations mutations, KVStore target) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        Preconditions.checkArgument(target != null, "null target");
        for (KeyRange remove : mutations.getRemoveRanges())
            target.removeRange(remove.getMin(), remove.getMax());
        for (Map.Entry<byte[], byte[]> entry : mutations.getPutPairs())
            target.put(entry.getKey(), entry.getValue());
        for (Map.Entry<byte[], Long> entry : mutations.getAdjustPairs())
            target.adjustCounter(entry.getKey(), entry.getValue());
    }

// Serialization

    /**
     * Serialize this instance.
     *
     * @param out output
     * @throws IOException if an error occurs
     */
    public void serialize(OutputStream out) throws IOException {

        // Removes
        this.removes.serialize(out);

        // Puts
        UnsignedIntEncoder.write(out, this.puts.size());
        byte[] prev = null;
        for (Map.Entry<byte[], byte[]> entry : this.puts.entrySet()) {
            final byte[] key = entry.getKey();
            final byte[] value = entry.getValue();
            KeyListEncoder.write(out, key, prev);
            KeyListEncoder.write(out, value, null);
            prev = key;
        }

        // Adjusts
        UnsignedIntEncoder.write(out, this.adjusts.size());
        prev = null;
        for (Map.Entry<byte[], Long> entry : this.adjusts.entrySet()) {
            final byte[] key = entry.getKey();
            final long value = entry.getValue();
            KeyListEncoder.write(out, key, prev);
            LongEncoder.write(out, value);
            prev = key;
        }
    }

    /**
     * Calculate the number of bytes required to serialize this instance via {@link #serialize serialize()}.
     *
     * @return number of serialized bytes
     */
    public long serializedLength() {

        // Removes
        long total = this.removes.serializedLength();

        // Puts
        total += UnsignedIntEncoder.encodeLength(this.puts.size());
        byte[] prev = null;
        for (Map.Entry<byte[], byte[]> entry : this.puts.entrySet()) {
            final byte[] key = entry.getKey();
            final byte[] value = entry.getValue();
            total += KeyListEncoder.writeLength(key, prev);
            total += KeyListEncoder.writeLength(value, null);
            prev = key;
        }

        // Adjusts
        total += UnsignedIntEncoder.encodeLength(this.adjusts.size());
        prev = null;
        for (Map.Entry<byte[], Long> entry : this.adjusts.entrySet()) {
            final byte[] key = entry.getKey();
            final long value = entry.getValue();
            total += KeyListEncoder.writeLength(key, prev);
            total += LongEncoder.encodeLength(value);
            prev = key;
        }

        // Done
        return total;
    }

    /**
     * Deserialize an instance created by {@link #serialize serialize()}.
     *
     * <p>
     * Any {@link IOException} thrown during iteration will be wrapped in a {@link RuntimeException}.
     * An {@link IllegalArgumentException} is also possible during iteration if malformed input is detected.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @return deserialized instance
     * @throws IllegalArgumentException if {@code input} is null
     * @throws IllegalArgumentException if malformed input is detected
     * @throws IOException if an I/O error occurs
     */
    public static Writes deserialize(InputStream input) throws IOException {
        Preconditions.checkArgument(input != null, "null input");

        // Create new instance
        final Writes writes = new Writes();

        // Populate removes
        writes.removes = KeyRanges.deserialize(input);

        // Populate puts
        int count = UnsignedIntEncoder.read(input);
        byte[] prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] key = KeyListEncoder.read(input, prev);
            final byte[] value = KeyListEncoder.read(input, null);
            writes.puts.put(key, value);
            prev = key;
        }

        // Populate adjusts
        count = UnsignedIntEncoder.read(input);
        prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] key = KeyListEncoder.read(input, prev);
            final long value = LongEncoder.read(input);
            writes.adjusts.put(key, value);
            prev = key;
        }

        // Done
        return writes;
    }

// SizeEstimating

    @Override
    public void addTo(SizeEstimator estimator) {
        estimator
          .addObjectOverhead()
          .addField(this.removes)
          .addTreeMapField(this.puts)
          .addTreeMapField(this.adjusts);
        for (Map.Entry<byte[], byte[]> entry : this.puts.entrySet()) {
            estimator
              .add(entry.getKey())
              .add(entry.getValue());
        }
        for (Map.Entry<byte[], Long> entry : this.adjusts.entrySet()) {
            estimator
              .add(entry.getKey())
              .addObjectOverhead()
              .addLongField();
        }
    }

// Object

    @Override
    public String toString() {
        final Converter<String, byte[]> byteConverter = ByteUtil.STRING_CONVERTER.reverse();
        final ConvertedNavigableMap<String, String, byte[], byte[]> putsView
          = new ConvertedNavigableMap<>(this.puts, byteConverter, byteConverter);
        final ConvertedNavigableMap<String, Long, byte[], Long> adjustsView
          = new ConvertedNavigableMap<>(this.adjusts, byteConverter, Converter.<Long>identity());
        return this.getClass().getSimpleName()
          + "[removes=" + this.removes
          + ",puts=" + putsView
          + (!this.adjusts.isEmpty() ? ",adjusts=" + adjustsView : "")
          + "]";
    }
}

