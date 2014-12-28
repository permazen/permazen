
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

/**
 * Reads bytes from a buffer.
 */
public class ByteReader {

    final byte[] buf;
    final int max;
    int off;

    /**
     * Constructor. The provided array is read from directly; no copy is made.
     *
     * @param buf array to read from
     * @throws NullPointerException if {@code buf} is null
     */
    public ByteReader(byte[] buf) {
        this.buf = buf;
        this.max = buf.length;
        this.off = 0;
    }

    /**
     * Constructor. The provided array is read from directly; no copy is made.
     *
     * @param buf array to read from
     * @param off starting offset into {@code buf}
     * @throws IndexOutOfBoundsException if {@code off} is out of bounds
     * @throws NullPointerException if {@code buf} is null
     */
    public ByteReader(byte[] buf, int off) {
        this(buf, off, buf.length - off);
    }

    /**
     * Constructor. The provided array is read from directly; no copy is made.
     *
     * @param buf array to read from
     * @param off offset into {@code buf}
     * @param len number of bytes to read
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} are out of bounds
     * @throws NullPointerException if {@code buf} is null
     */
    public ByteReader(byte[] buf, int off, int len) {
        if (off < 0 || len < 0 || off > buf.length || off + len < 0 || off + len > buf.length)
            throw new IndexOutOfBoundsException("buf.length = " + buf.length + ", off = " + off + ", len = " + len);
        this.buf = buf;
        this.max = off + len;
        this.off = off;
    }

    /**
     * Constructor. Takes a snapshot of the given writer's entire content.
     *
     * @param writer {@code ByteWriter} to read data from
     * @throws NullPointerException if {@code writer} is null
     */
    public ByteReader(ByteWriter writer) {
        this(writer.buf, 0, writer.len);
    }

    /**
     * Constructor. Takes a snapshot of the given writer's content starting at the specified position.
     *
     * @param writer {@code ByteWriter} to read data from
     * @param mark position previously returned by {@code ByteWriter#mark}
     * @throws IndexOutOfBoundsException if {@code mark} is out of bounds
     * @throws NullPointerException if {@code writer} is null
     */
    public ByteReader(ByteWriter writer, int mark) {
        this(writer.buf, mark, writer.len - mark);
    }

    /**
     * Peek at next byte, if any.
     *
     * @return next byte (0-255)
     * @throws IndexOutOfBoundsException if there are no more bytes
     */
    public int peek() {
        if (this.off == this.max)
            throw new IndexOutOfBoundsException();
        return this.buf[this.off] & 0xff;
    }

    /**
     * Read the next byte.
     *
     * @return next byte (0-255)
     * @throws IndexOutOfBoundsException if there are no more bytes
     */
    public int readByte() {
        if (this.off == this.max)
            throw new IndexOutOfBoundsException();
        return this.buf[this.off++] & 0xff;
    }

    /**
     * Unread the previously read byte. Equivalent to {@code unread(1)}.
     *
     * @throws IndexOutOfBoundsException if there are no more bytes to unread
     */
    public void unread() {
        if (this.off == 0)
            throw new IndexOutOfBoundsException();
        this.off--;
    }

    /**
     * Unread the given number of previously read bytes.
     *
     * @throws IndexOutOfBoundsException if there are no more bytes to unread
     */
    public void unread(int len) {
        if (this.off - len < 0)
            throw new IndexOutOfBoundsException();
        this.off -= len;
    }

    /**
     * Read the specified number of bytes.
     *
     * @param len number of bytes to read
     * @return bytes read
     * @throws IndexOutOfBoundsException if there are not enough bytes
     * @throws IllegalArgumentException if {@code len} is negative
     */
    public byte[] readBytes(int len) {
        if (len < 0)
            throw new IllegalArgumentException("len < 0");
        if (this.off + len > this.max)
            throw new IndexOutOfBoundsException();
        final byte[] result = new byte[len];
        System.arraycopy(this.buf, this.off, result, 0, len);
        this.off += len;
        return result;
    }

    /**
     * Get the number of bytes remaining.
     */
    public int remain() {
        return this.max - this.off;
    }

    /**
     * Skip over bytes.
     *
     * @throws IndexOutOfBoundsException if {@code num} is negative
     * @throws IndexOutOfBoundsException if less than {@code num} bytes remain
     */
    public void skip(int num) {
        if (num < 0 || this.off + num > this.max)
            throw new IndexOutOfBoundsException();
        this.off += num;
    }

    /**
     * Get current offset into buffer.
     */
    public int getOffset() {
        return this.off;
    }

    /**
     * Get maximum offset into buffer.
     */
    public int getMax() {
        return this.max;
    }

    /**
     * Copy a range of bytes from the buffer. Does not change the read position.
     *
     * @param off offset into buffer
     * @param len number of bytes
     * @throws IndexOutOfBoundsException if {@code off} and/or {@code len} is out of bounds
     */
    public byte[] getBytes(int off, int len) {
        final byte[] data = new byte[len];
        System.arraycopy(this.buf, off, data, 0, len);
        return data;
    }

    /**
     * Copy a range of bytes from the given offset to the end of the buffer. Does not change the read position.
     *
     * @param off offset into buffer
     * @throws IndexOutOfBoundsException if {@code off} is out of bounds
     */
    public byte[] getBytes(int off) {
        return this.getBytes(off, this.max - off);
    }

    /**
     * Copy all the of bytes in the buffer. Does not change the read position.
     */
    public byte[] getBytes() {
        return this.max == this.buf.length ? this.buf.clone() : this.getBytes(0);
    }

    /**
     * Mark current read position.
     */
    public int mark() {
        return this.off;
    }

    /**
     * Reset read position to a previously marked position.
     *
     * @param mark value previously returned by {@link #mark}
     * @throws IndexOutOfBoundsException if {@code mark} is out of bounds
     */
    public void reset(int mark) {
        if (mark < 0 || mark > this.max)
            throw new IndexOutOfBoundsException();
        this.off = mark;
    }
}

