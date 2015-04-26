
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
import java.util.Arrays;
import java.util.Iterator;
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
public class Writes {

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
     * The caller must not modify any of the {@code byte[]} arrays in the returned map.
     * </p>
     *
     * @return unmodifiable mapping from key to corresponding value
     */
    public NavigableMap<byte[], byte[]> getPuts() {
        return this.puts;
    }

    /**
     * Get the set of counter adjustments contained by this instance.
     *
     * <p>
     * The caller must not modify any of the {@code byte[]} arrays in the returned map.
     * </p>
     *
     * @return mapping from key to corresponding counter adjustment
     */
    public NavigableMap<byte[], Long> getAdjusts() {
        return this.adjusts;
    }

    /**
     * Clear all mutations.
     */
    public void clear() {
        this.removes = KeyRanges.EMPTY;
        this.puts.clear();
        this.adjusts.clear();
    }

// Application

    /**
     * Apply all mutations contained in this instance to the given {@link KVStore}.
     *
     * <p>
     * Mutations are applied in this order: removes, puts, counter adjustments.
     * Within each group, mutations are applied in key order.
     *
     * @param target target for recorded mutations
     * @throws IllegalArgumentException if {@code target} is null
     */
    public void applyTo(KVStore target) {
        if (target == null)
            throw new IllegalArgumentException("null target");
        for (KeyRange remove : this.removes.asList())
            target.removeRange(remove.getMin(), remove.getMax());
        for (Map.Entry<byte[], byte[]> entry : this.puts.entrySet())
            target.put(entry.getKey(), entry.getValue());
        for (Map.Entry<byte[], Long> entry : this.adjusts.entrySet())
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
     * Deserialize an instance created by {@link #serialize serialize()}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @return deserialized instance
     * @throws IOException if an I/O error occurs
     * @throws java.io.EOFException if the input ends unexpectedly
     * @throws IllegalArgumentException if malformed input is detected
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static Writes deserialize(InputStream input) throws IOException {

        // Sanity check
        if (input == null)
            throw new IllegalArgumentException("null input");

        // Create instance
        final Writes writes = new Writes();

        // Removes
        writes.removes = KeyRanges.deserialize(input);

        // Puts
        int count = UnsignedIntEncoder.read(input);
        byte[] prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] key = KeyListEncoder.read(input, prev);
            final byte[] value = KeyListEncoder.read(input, null);
            writes.puts.put(key, value);
            prev = key;
        }

        // Adjusts
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

    /**
     * Deserialize an instance created by {@link #serialize serialize()} and apply it to the given {@link KVStore}.
     * This method executes in an online fashion.
     *
     * @param target target for mutations
     * @throws IllegalArgumentException if {@code target} is null
     * @param input input stream containing data from {@link #serialize serialize()}
     * @throws IOException if an I/O error occurs
     * @throws java.io.EOFException if the input ends unexpectedly
     * @throws IllegalArgumentException if malformed input is detected
     * @throws IllegalArgumentException if {@code target} or {@code input} is null
     */
    public static void deserializeAndApply(KVStore target, InputStream input) throws IOException {

        // Sanity check
        if (target == null)
            throw new IllegalArgumentException("null target");
        if (input == null)
            throw new IllegalArgumentException("null input");

        // Removes
        for (Iterator<KeyRange> i = KeyRanges.readIterator(input); i.hasNext(); ) {
            final KeyRange range;
            try {
                range = i.next();
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException)
                    throw (IOException)e.getCause();
                throw e;
            }
            final byte[] min = range.getMin();
            final byte[] max = range.getMax();
            if (max != null && Arrays.equals(max, ByteUtil.getNextKey(min)))
                target.remove(min);
            else
                target.removeRange(min, max);
        }

        // Puts
        int count = UnsignedIntEncoder.read(input);
        byte[] prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] key = KeyListEncoder.read(input, prev);
            final byte[] value = KeyListEncoder.read(input, null);
            target.put(key, value);
            prev = key;
        }

        // Adjusts
        count = UnsignedIntEncoder.read(input);
        prev = null;
        for (int i = 0; i < count; i++) {
            final byte[] key = KeyListEncoder.read(input, prev);
            final long value = LongEncoder.read(input);
            target.adjustCounter(key, value);
            prev = key;
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
          + "[puts=" + putsView
          + (!this.removes.isEmpty() ? ",removes=" + this.removes : "")
          + (!this.adjusts.isEmpty() ? ",adjusts=" + adjustsView : "")
          + "]";
    }
}

