
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.security.SecureRandom;
import java.util.regex.Pattern;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Object IDs. Instances identify individual {@link Database} objects.
 */
public class ObjId implements Comparable<ObjId> {

    /**
     * The number of bytes in the binary encoding of an {@link ObjId}.
     */
    public static final int NUM_BYTES = 8;

    /**
     * Regular expression that matches the string encoding of an {@link ObjId}.
     */
    public static final Pattern PATTERN = Pattern.compile("\\p{XDigit}{" + (NUM_BYTES * 2) + "}");

    private static final ThreadLocal<SecureRandom> RANDOM = new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };

    private final long value;

    /**
     * Create a new, random instance with the given storage ID.
     *
     * @param storageId storage ID, must be greater than zero
     * @throws IllegalArgumentException if {@code storageId} is zero or negative
     */
    public ObjId(int storageId) {
        this(ObjId.buildRandom(storageId));
    }

    /**
     * Constructor that parses a string previously returned by {@link #toString}.
     *
     * @param string string encoding of an instance
     * @throws IllegalArgumentException if {@code string} is invalid
     */
    public ObjId(String string) {
        this(ObjId.parseString(string));
    }

    /**
     * Constructor that reads an encoded instance from the given {@link ByteReader}.
     *
     * @param reader input for binary encoding of an instance
     * @throws IllegalArgumentException if {@code reader} contains invalid data
     */
    public ObjId(ByteReader reader) {
        if (reader == null)
            throw new IllegalArgumentException("null reader");
        this.value = ByteUtil.readLong(reader);
        final int storageId;
        try {
            storageId = this.getStorageId();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid object ID", e);
        }
        if (storageId <= 0)
            throw new IllegalArgumentException("invalid object ID containing storage ID " + storageId);
    }

    /**
     * Get the storage ID associated with this instance.
     */
    public int getStorageId() {
        return UnsignedIntEncoder.read(new ByteReader(this.getBytes()));
    }

    /**
     * Get the binary encoding of this instance.
     */
    public byte[] getBytes() {
        final ByteWriter writer = new ByteWriter(NUM_BYTES);
        this.writeTo(writer);
        return writer.getBytes();
    }

    /**
     * Get this instance encoded as a {@code long} value.
     */
    public long asLong() {
        return this.value;
    }

    /**
     * Write the binary encoding of this instance to the given output.
     */
    public void writeTo(ByteWriter writer) {
        ByteUtil.writeLong(writer, this.value);
    }

    /**
     * Get the smallest (i.e., first) instance having the given storage ID.
     *
     * @param storageId storage ID, must be greater than zero
     * @return smallest instance with storage ID {@code storageId} (inclusive)
     * @throws IllegalArgumentException if {@code storageId} is zero or negative
     */
    public static ObjId getMin(int storageId) {
        return ObjId.getFill(storageId, 0x00);
    }

    /**
     * Get the largest (i.e., last) instance having the given storage ID.
     *
     * @param storageId storage ID, must be greater than zero
     * @return largest instance with storage ID {@code storageId} (inclusive)
     * @throws IllegalArgumentException if {@code storageId} is zero or negative
     */
    public static ObjId getMax(int storageId) {
        return ObjId.getFill(storageId, 0xff);
    }

    /**
     * Get the {@link KeyRange} containing all object IDs with the given storage ID.
     *
     * @param storageId storage ID, must be greater than zero
     * @return {@link KeyRange} containing all object IDs having the specified type
     * @throws IllegalArgumentException if {@code storageId} is zero or negative
     */
    public static KeyRange getKeyRange(int storageId) {
        final ByteWriter writer = new ByteWriter(NUM_BYTES);
        UnsignedIntEncoder.write(writer, storageId);
        return KeyRange.forPrefix(writer.getBytes());
    }

    private static ObjId getFill(int storageId, int value) {
        if (storageId <= 0)
            throw new IllegalArgumentException("invalid storage ID " + storageId);
        final ByteWriter writer = new ByteWriter(NUM_BYTES);
        UnsignedIntEncoder.write(writer, storageId);
        for (int remain = NUM_BYTES - writer.getLength(); remain > 0; remain--)
            writer.writeByte(value);
        return new ObjId(new ByteReader(writer));
    }

// Object

    /**
     * Encode this instance as a string.
     */
    @Override
    public String toString() {
        final ByteWriter writer = new ByteWriter(NUM_BYTES);
        this.writeTo(writer);
        final byte[] buf = writer.getBytes();
        final char[] result = new char[NUM_BYTES * 2];
        int off = 0;
        for (int i = 0; i < buf.length; i++) {
            int b = buf[i];
            result[off++] = Character.forDigit((b >> 4) & 0x0f, 16);
            result[off++] = Character.forDigit(b & 0x0f, 16);
        }
        return new String(result);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ObjId that = (ObjId)obj;
        return this.value == that.value;
    }

    @Override
    public int hashCode() {
        return (int)(this.value >> 32) ^ (int)this.value;
    }

// Comparable

    @Override
    public int compareTo(ObjId that) {
        return ByteUtil.compare(this.getBytes(), that.getBytes());
    }

// Internal methods

    private static ByteReader buildRandom(int storageId) {
        if (storageId <= 0)
            throw new IllegalArgumentException("invalid storage ID " + storageId);
        final ByteWriter writer = new ByteWriter(NUM_BYTES);
        UnsignedIntEncoder.write(writer, storageId);
        final byte[] randomPart = new byte[NUM_BYTES - writer.getLength()];
        ObjId.RANDOM.get().nextBytes(randomPart);
        writer.write(randomPart);
        return new ByteReader(writer);
    }

    private static ByteReader parseString(String string) {
        if (string == null)
            throw new IllegalArgumentException("null string");
        if (string.length() != NUM_BYTES * 2)
            throw new IllegalArgumentException("invalid object ID `" + string + "'");
        final byte[] buf = new byte[NUM_BYTES];
        int off = 0;
        for (int i = 0; i < buf.length; i++) {
            final int digit1 = Character.digit(string.charAt(off++), 16);
            final int digit2 = Character.digit(string.charAt(off++), 16);
            if (digit1 == -1 || digit2 == -1)
                throw new IllegalArgumentException("invalid object ID `" + string + "'");
            buf[i] = (byte)((digit1 << 4) | digit2);
        }
        return new ByteReader(buf);
    }
}

