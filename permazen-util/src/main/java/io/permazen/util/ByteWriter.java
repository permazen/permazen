
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.Objects;

/**
 * Writes bytes to a buffer.
 */
public class ByteWriter {

    private static final int DEFAULT_CAPACITY = 20;

    byte[] buf;
    int len;

    /**
     * Default constructor.
     */
    public ByteWriter() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Constructor.
     *
     * @param capacity initial capacity of buffer
     */
    public ByteWriter(int capacity) {
        if (capacity < 0)
            throw new IndexOutOfBoundsException("capacity = " + capacity);
        this.buf = new byte[capacity];
    }

    /**
     * Retrieve all of the bytes that have been written to this instance.
     *
     * @return byte content written so far (not necessarily a copy; caller must not modify)
     */
    public byte[] getBytes() {
        if (this.buf.length == this.len)
            return this.buf;
        final byte[] result = new byte[this.len];
        System.arraycopy(this.buf, 0, result, 0, this.len);
        return result;
    }

    /**
     * Retrieve the bytes that have been written to this instance, starting at the given offset.
     *
     * @param off offset into written bytes
     * @return byte content written so far starting at {@code off} (not necessarily a copy; caller must not modify)
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} is out of bounds
     */
    public byte[] getBytes(int off) {
        return this.getBytes(off, this.len - off);
    }

    /**
     * Retrieve a sub-range of the bytes that have been written to this instance.
     *
     * @param off offset into written bytes
     * @param len desired length
     * @return {@code len} bytes written so far starting from {@code off} (not necessarily a copy; caller must not modify)
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} is out of bounds
     */
    public byte[] getBytes(int off, int len) {
        if (off == 0 && len == this.len && this.buf.length == this.len)
            return this.buf;
        Objects.checkFromIndexSize(off, len, this.len);
        final byte[] result = new byte[len];
        System.arraycopy(this.buf, off, result, 0, len);
        return result;
    }

    /**
     * Write a single byte to this instance.
     *
     * @param value byte to write; all but the lower 8 bits are ignored
     */
    public void writeByte(int value) {
        final byte b = (byte)value;
        this.makeRoom(1);
        this.buf[this.len++] = b;
    }

    /**
     * Read all remaining content from the given {@link ByteReader} and write it to this instance.
     *
     * @param reader source for bytes to write
     */
    public void write(ByteReader reader) {
        this.write(reader.buf, reader.off, reader.max - reader.off);
    }

    /**
     * Write an array of bytes to this instance.
     *
     * @param data bytes to write
     */
    public void write(byte[] data) {
        this.makeRoom(data.length);
        System.arraycopy(data, 0, this.buf, this.len, data.length);
        this.len += data.length;
    }

    /**
     * Write a sub-range from an array of bytes.
     *
     * @param data bytes to write
     * @param off offset into {@code data}
     * @param len the number of bytes to write
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} is out of bounds
     */
    public void write(byte[] data, int off, int len) {
        Objects.checkFromIndexSize(off, len, data.length);
        this.makeRoom(len);
        System.arraycopy(data, off, this.buf, this.len, len);
        this.len += len;
    }

    /**
     * Get the current buffer length. Returns the same value as {@code #mark}.
     *
     * @return number of bytes written so far
     */
    public int getLength() {
        return this.len;
    }

    /**
     * Mark current position. Returns the same value as {@code #getLength}.
     *
     * @return number of bytes written so far
     */
    public int mark() {
        return this.len;
    }

    /**
     * Reset write position to a previously marked position.
     *
     * @param mark value previously returned by {@link #mark}
     * @throws IndexOutOfBoundsException if {@code mark} is out of bounds
     */
    public void reset(int mark) {
        Objects.checkIndex(mark, this.len);
        this.len = mark;
    }

    /**
     * Make room for additional bytes.
     *
     * @param amount number of additional bytes
     */
    void makeRoom(int amount) {
        if (this.len + amount > this.buf.length) {
            final byte[] newbuf = new byte[Math.max(this.len + amount, this.buf.length * 2 + 8)];
            System.arraycopy(this.buf, 0, newbuf, 0, this.len);
            this.buf = newbuf;
        }
    }
}
