
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.io.InputStream;
import java.util.Objects;

/**
 * Reads bytes from a buffer.
 */
public class ByteReader {

    final byte[] buf;
    final int max;
    int off;

// Constructors

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
        Objects.checkFromIndexSize(off, len, buf.length);
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
     * @param mark position previously returned by {@link ByteWriter#mark}
     * @throws IndexOutOfBoundsException if {@code mark} is out of bounds
     * @throws NullPointerException if {@code writer} is null
     */
    public ByteReader(ByteWriter writer, int mark) {
        this(writer.buf, mark, writer.len - mark);
    }

// Methods

    /**
     * Peek at next byte, if any.
     *
     * @return next byte (0-255)
     * @throws IndexOutOfBoundsException if there are no more bytes
     */
    public int peek() {
        if (this.off == this.max)
            throw new IndexOutOfBoundsException("truncated input");
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
            throw new IndexOutOfBoundsException("truncated input");
        return this.buf[this.off++] & 0xff;
    }

    /**
     * Unread the previously read byte. Equivalent to {@code unread(1)}.
     *
     * @throws IndexOutOfBoundsException if there are no more bytes to unread
     */
    public void unread() {
        if (this.off == 0)
            throw new IndexOutOfBoundsException("not enough previous bytes");
        this.off--;
    }

    /**
     * Unread the given number of previously read bytes.
     *
     * @param len the number of bytes to unread
     * @throws IndexOutOfBoundsException if there are no more bytes to unread
     */
    public void unread(int len) {
        if (this.off - len < 0)
            throw new IndexOutOfBoundsException("not enough previous bytes");
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
        Objects.checkFromIndexSize(this.off, len, this.max);
        final byte[] result = new byte[len];
        System.arraycopy(this.buf, this.off, result, 0, len);
        this.off += len;
        return result;
    }

    /**
     * Read all the of remaining bytes and advance the read position to the end.
     *
     * @return copy of the remaining data
     */
    public byte[] readRemaining() {
        return this.readBytes(this.remain());
    }

    /**
     * Get the number of bytes remaining.
     *
     * @return bytes remaining
     */
    public int remain() {
        return this.max - this.off;
    }

    /**
     * Skip over bytes.
     *
     * @param num the number of bytes to skip
     * @throws IndexOutOfBoundsException if {@code num} is negative
     * @throws IndexOutOfBoundsException if less than {@code num} bytes remain
     */
    public void skip(int num) {
        Objects.checkFromIndexSize(this.off, num, this.max);
        this.off += num;
    }

    /**
     * Get current offset into buffer.
     *
     * @return current offset
     */
    public int getOffset() {
        return this.off;
    }

    /**
     * Get maximum offset into buffer.
     *
     * @return maximum offset
     */
    public int getMax() {
        return this.max;
    }

    /**
     * Copy a range of bytes from the buffer. Does not change the read position.
     *
     * @param off offset into buffer
     * @param len number of bytes
     * @return copy of the specified byte range
     * @throws IndexOutOfBoundsException if {@code off} and/or {@code len} is out of bounds
     */
    public byte[] getBytes(int off, int len) {
        Objects.checkFromIndexSize(off, len, this.max);
        final byte[] data = new byte[len];
        System.arraycopy(this.buf, off, data, 0, len);
        return data;
    }

    /**
     * Copy a range of bytes from the given offset to the end of the buffer. Does not change the read position.
     *
     * @param off offset into buffer
     * @return copy of the specified byte range
     * @throws IndexOutOfBoundsException if {@code off} is out of bounds
     */
    public byte[] getBytes(int off) {
        return this.getBytes(off, this.max - off);
    }

    /**
     * Copy all the of bytes in the buffer. Does not change the read position.
     *
     * @return copy of the entire buffer
     */
    public byte[] getBytes() {
        return this.max == this.buf.length ? this.buf.clone() : this.getBytes(0);
    }

    /**
     * Mark current read position.
     *
     * @return the current offset
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
        Objects.checkIndex(mark, this.max);
        this.off = mark;
    }

    /**
     * Return a view of this instance as an {@link InputStream}.
     *
     * @return streaming view of this instance
     */
    public InputStream asInputStream() {
        return new InputStream() {

            private int mark;

            @Override
            public int read() {
                try {
                    return ByteReader.this.readByte();
                } catch (IndexOutOfBoundsException e) {
                    return -1;
                }
            }

            @Override
            public int read(byte[] buf, int off, int len) {
                Objects.checkFromIndexSize(off, len, buf.length);
                final int remain = ByteReader.this.remain();
                if (remain == 0)
                    return -1;
                len = Math.min(len, remain);
                System.arraycopy(ByteReader.this.buf, ByteReader.this.off, buf, off, len);
                ByteReader.this.off += len;
                return len;
            }

            @Override
            public long skip(long num) {
                final int actual = (int)Math.min(num, ByteReader.this.remain());
                ByteReader.this.skip(actual);
                return actual;
            }

            @Override
            public int available() {
                return ByteReader.this.remain();
            }

            @Override
            public void close() {
            }

            @Override
            public void mark(int readlimit) {
                this.mark = ByteReader.this.mark();
            }

            @Override
            public void reset() {
                ByteReader.this.reset(this.mark);
            }

            @Override
            public boolean markSupported() {
                return true;
            }
        };
    }
}
