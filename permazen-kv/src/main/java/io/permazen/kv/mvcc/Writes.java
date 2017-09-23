
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.util.KeyListEncoder;
import io.permazen.util.ByteUtil;
import io.permazen.util.ConvertedNavigableMap;
import io.permazen.util.ImmutableNavigableMap;
import io.permazen.util.LongEncoder;
import io.permazen.util.UnsignedIntEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;

/**
 * Holds a set of writes to a {@link KVStore}.
 *
 * <p>
 * Each mutation is either a key/value put, the removal of a key range (possibly containing only a single key),
 * or a counter adjustment.
 *
 * <p>
 * Instances are not thread safe.
 */
public class Writes implements Cloneable, Mutations {

    private /*final*/ KeyRanges removes;
    private /*final*/ NavigableMap<byte[], byte[]> puts;
    private /*final*/ NavigableMap<byte[], Long> adjusts;
    private /*final*/ boolean immutable;

    public Writes() {
        this(KeyRanges.empty(), new TreeMap<>(ByteUtil.COMPARATOR), new TreeMap<>(ByteUtil.COMPARATOR), false);
    }

    private Writes(KeyRanges removes,
      NavigableMap<byte[], byte[]> puts, NavigableMap<byte[], Long> adjusts, boolean immutable) {
        this.removes = removes;
        this.puts = puts;
        this.adjusts = adjusts;
        this.immutable = immutable;
    }

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
     * Get the written key/value pairs contained by this instance.
     *
     * <p>
     * The caller must not modify any of the returned {@code byte[]} arrays.
     *
     * @return mapping from key to corresponding value
     */
    public NavigableMap<byte[], byte[]> getPuts() {
        return this.puts;
    }

    /**
     * Get the set of counter adjustments contained by this instance.
     *
     * <p>
     * The caller must not modify any of the returned {@code byte[]} arrays.
     *
     * @return mapping from key to corresponding counter adjustment
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
        this.removes.clear();
        this.puts.clear();
        this.adjusts.clear();
    }

// Mutations

    @Override
    public NavigableSet<KeyRange> getRemoveRanges() {
        return this.removes.asSet();
    }

    @Override
    public Iterable<Map.Entry<byte[], byte[]>> getPutPairs() {
        return this.getPuts().entrySet();
    }

    @Override
    public Iterable<Map.Entry<byte[], Long>> getAdjustPairs() {
        return this.getAdjusts().entrySet();
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
     * @deprecated use {@link KVStore#apply} instead
     * @param mutations mutations to apply
     * @param target target for mutations
     * @throws IllegalArgumentException if either parameter is null
     * @throws UnsupportedOperationException if this instance is immutable
     */
    @Deprecated
    public static void apply(Mutations mutations, KVStore target) {
        Preconditions.checkArgument(target != null, "null target");
        target.apply(mutations);
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
     * Deserialize a mutable instance created by {@link #serialize serialize()}.
     *
     * <p>
     * Equivalent to {@link #deserialize(InputStream, boolean) deserialize}{@code (input, false)}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @return mutable deserialized instance
     * @throws IllegalArgumentException if {@code input} is null
     * @throws IllegalArgumentException if malformed input is detected
     * @throws IOException if an I/O error occurs
     */
    public static Writes deserialize(InputStream input) throws IOException {
        return Writes.deserialize(input, false);
    }

    /**
     * Deserialize an instance created by {@link #serialize serialize()}.
     *
     * @param input input stream containing data from {@link #serialize serialize()}
     * @param immutable true for an immutable instance, otherwise false
     * @return deserialized instance
     * @throws IllegalArgumentException if {@code input} is null
     * @throws IllegalArgumentException if malformed input is detected
     * @throws IOException if an I/O error occurs
     */
    public static Writes deserialize(InputStream input, boolean immutable) throws IOException {
        Preconditions.checkArgument(input != null, "null input");

        // Get removes
        final KeyRanges removes = new KeyRanges(input, immutable);

        // Get puts
        final int putCount = UnsignedIntEncoder.read(input);
        final byte[][] putKeys = new byte[putCount][];
        final byte[][] putVals = new byte[putCount][];
        byte[] prev = null;
        for (int i = 0; i < putCount; i++) {
            putKeys[i] = KeyListEncoder.read(input, prev);
            putVals[i] = KeyListEncoder.read(input, null);
            prev = putKeys[i];
        }
        final NavigableMap<byte[], byte[]> puts;
        if (immutable)
            puts = new ImmutableNavigableMap<>(putKeys, putVals, ByteUtil.COMPARATOR);
        else {
            puts = new TreeMap<>(ByteUtil.COMPARATOR);
            for (int i = 0; i < putCount; i++)
                puts.put(putKeys[i], putVals[i]);
        }

        // Get adjusts
        final int adjCount = UnsignedIntEncoder.read(input);
        final byte[][] adjKeys = new byte[adjCount][];
        final Long[] adjVals = new Long[adjCount];
        prev = null;
        for (int i = 0; i < adjCount; i++) {
            adjKeys[i] = KeyListEncoder.read(input, prev);
            adjVals[i] = LongEncoder.read(input);
            prev = adjKeys[i];
        }
        final NavigableMap<byte[], Long> adjusts;
        if (immutable)
            adjusts = new ImmutableNavigableMap<>(adjKeys, adjVals, ByteUtil.COMPARATOR);
        else {
            adjusts = new TreeMap<>(ByteUtil.COMPARATOR);
            for (int i = 0; i < adjCount; i++)
                adjusts.put(adjKeys[i], adjVals[i]);
        }

        // Done
        return new Writes(removes, puts, adjusts, immutable);
    }

// Cloneable

    /**
     * Clone this instance.
     *
     * <p>
     * This is a "mostly deep" clone: all of the mutations are copied, but the actual
     * {@code byte[]} keys and values, which are already assumed non-mutable, are not copied.
     *
     * <p>
     * The returned clone will always be mutable, even if this instance is not.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Writes clone() {
        final Writes clone;
        try {
            clone = (Writes)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.removes = this.removes.clone();
        clone.puts = new TreeMap<>(clone.puts);
        clone.adjusts = new TreeMap<>(clone.adjusts);
        clone.immutable = false;
        return clone;
    }

    /**
     * Return an immutable snapshot of this instance.
     *
     * @return immutable snapshot
     */
    public Writes immutableSnapshot() {
        if (this.immutable)
            return this;
        return new Writes(this.removes.immutableSnapshot(),
          new ImmutableNavigableMap<>(this.puts), new ImmutableNavigableMap<>(this.adjusts), true);
    }

// Object

    @Override
    public String toString() {
        final Converter<String, byte[]> byteConverter = ByteUtil.STRING_CONVERTER.reverse();
        final ConvertedNavigableMap<String, String, byte[], byte[]> putsView
          = new ConvertedNavigableMap<>(this.puts, byteConverter, byteConverter);
        final ConvertedNavigableMap<String, Long, byte[], Long> adjustsView
          = new ConvertedNavigableMap<>(this.adjusts, byteConverter, Converter.<Long>identity());
        final StringBuilder buf = new StringBuilder();
        buf.append(this.getClass().getSimpleName())
          .append("[removes=")
          .append(this.removes);
        if (!this.puts.isEmpty()) {
            buf.append(",puts=");
            this.appendEntries(buf, putsView);
        }
        if (!this.adjusts.isEmpty()) {
            buf.append(",adjusts=");
            this.appendEntries(buf, adjustsView);
        }
        buf.append("]");
        return buf.toString();
    }

    private void appendEntries(StringBuilder buf, Map<String, ?> map) {
        buf.append('{');
        int index = 0;
    entryLoop:
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            final String key = entry.getKey();
            final Object val = entry.getValue();
            switch (index++) {
            case 0:
                break;
            case 32:
                buf.append("...");
                break entryLoop;
            default:
                buf.append(", ");
                break;
            }
            buf.append(this.truncate(key, 32))
              .append('=')
              .append(this.truncate(String.valueOf(val), 32));
        }
    }

    private String truncate(String s, int max) {
        if (s == null || s.length() <= max)
            return s;
        return s.substring(0, max) + "...";
    }
}

