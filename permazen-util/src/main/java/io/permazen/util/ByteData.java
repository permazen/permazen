
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * Zero or more bytes in read-only form.
 *
 * <p>
 * Instances are thread-safe and immutable.
 *
 * <p>
 * Instances {@linkplain #compareTo order themselves} using unsigned lexical comparison.
 */
public final class ByteData implements Comparable<ByteData> {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final ByteData EMPTY = new ByteData(EMPTY_BYTE_ARRAY);

    private static final int NUM_ZEROES = 16;
    private static final ByteData ZEROS = new ByteData(new byte[NUM_ZEROES]);

    private static final int MAX_TOSTRING_HEXBYTES = 64;

    private final byte[] data;      // the data in here must never change
    private final int min;
    private final int max;

    private int hash;

// Constructors

    private ByteData(byte[] data) {
        this.data = data;
        this.min = 0;
        this.max = data.length;
    }

    private ByteData(byte[] data, int min, int max) {
        Objects.checkFromToIndex(min, max, data.length);
        this.data = data;
        this.min = min;
        this.max = max;
    }

// Public Static Methods

    /**
     * Obtain an instance containing a copy of the given {@code byte[]} array.
     *
     * @param data byte data
     * @return instance containing a copy of {@code data}
     * @throws IllegalArgumentException if {@code data} is null
     */
    public static ByteData of(byte... data) {
        Preconditions.checkArgument(data != null, "null data");
        return new ByteData(data.clone());
    }

    /**
     * Obtain an instance containing a the given byte data.
     *
     * @param data byte data as integers; all but the lower 8 bits are ignored
     * @return instance containing a copy of {@code data}
     * @throws IllegalArgumentException if {@code data} is null
     */
    public static ByteData of(int... data) {
        Preconditions.checkArgument(data != null, "null data");
        final byte[] buf = new byte[data.length];
        for (int i = 0; i < data.length; i++)
            buf[i] = (byte)data[i];
        return new ByteData(buf);
    }

    /**
     * Obtain an instance containing a copy of the given {@code byte[]} array region.
     *
     * @param data byte data
     * @param off offset into {@code data}
     * @param len number of bytes
     * @return instance containing a copy of the specified region of {@code data}
     * @throws IllegalArgumentException if {@code data} is null
     * @throws IndexOutOfBoundsException if {@code off} and/or {@code len} is out of bounds
     */
    public static ByteData of(byte[] data, int off, int len) {
        Preconditions.checkArgument(data != null, "null data");
        return new ByteData(data.clone(), off, off + len);
    }

    /**
     * Return an instance decoded from the given string of hex digits.
     *
     * <p>
     * Digits greater than 9 may be uppercase or lowercase.
     *
     * @param hex zero or more hexadeximal digits
     * @return {@code hex} decoded as hexadeximal
     * @throws IllegalArgumentException if any character in {@code hex} is not a hex digit
     * @throws IllegalArgumentException if {@code hex} does not have even length
     * @throws IllegalArgumentException if {@code hex} is null
     */
    public static ByteData fromHex(String hex) {
        Preconditions.checkArgument(hex != null, "null hex");
        final int hexLen = hex.length();
        if (hexLen == 0)
            return ByteData.EMPTY;
        Preconditions.checkArgument((hexLen & 1) == 0, "odd number of digits");
        final byte[] data = new byte[hexLen / 2];
        int hexOff = 0;
        int dataOff = 0;
        while (hexOff < hexLen) {
            final char ch1 = hex.charAt(hexOff++);
            final char ch2 = hex.charAt(hexOff++);
            final int nib1 = Character.digit(ch1, 16);
            final int nib2 = Character.digit(ch2, 16);
            if (nib1 == -1 || nib2 == -1)
                throw new IllegalArgumentException(String.format("invalid hex value '%c%c' at offset %d", ch1, ch2, hexOff - 2));
            data[dataOff++] = (byte)((nib1 << 4) | nib2);
        }
        return new ByteData(data);
    }

    /**
     * Obtain an empty instance.
     *
     * @return an empty instance
     */
    public static ByteData empty() {
        return ByteData.EMPTY;
    }

    /**
     * Obtain an instance containing the given number of zero bytes.
     *
     * @param len number of zero bytes
     * @return instance containing {@code len} zero bytes
     * @throws IllegalArgumentException if {@code len} is negative
     */
    public static ByteData zeros(int len) {
        Preconditions.checkArgument(len >= 0, "negative len");
        return len == 0 ? ByteData.EMPTY : len <= NUM_ZEROES ? ByteData.ZEROS.substring(0, len) : new ByteData(new byte[len]);
    }

    /**
     * Create a new {@link Writer} using the default initial capacity.
     *
     * @return new data writer
     */
    public static Writer newWriter() {
        return new Writer();
    }

    /**
     * Create a new {@link Writer} using the given initial capacity.
     *
     * @param initialCapacity intiial buffer capcity
     * @return new data writer
     * @throws IllegalArgumentException if {@code initialCapacity} is negative
     */
    public static Writer newWriter(int initialCapacity) {
        return new Writer(initialCapacity);
    }

    /**
     * Determine how many identical bytes there are at specified offsets in two instances.
     *
     * <p>
     * A starting offset is given for each instance. This method returns the number of consecutive pairs of bytes that are
     * equal in both instances, starting at the given offsets. The comparison stops when the a non-equal byte pair is found,
     * or one of the offsets would exceed the length of the corresponding instance.
     *
     * @param data1 first instance
     * @param data2 first instance
     * @param off1 starting offset in {@code data1}
     * @param off2 starting offset in {@code data2}
     * @return the number of bytes that agree starting at {@code off1} in {@code data1} and {@code off2} in {@code data2}
     * @throws IllegalArgumentException if {@code data1} or {@code data2} is null
     * @throws IllegalArgumentException if {@code off1} or {@code off2} is out of bounds
     */
    public static int numEqual(ByteData data1, int off1, ByteData data2, int off2) {
        Preconditions.checkArgument(data1 != null, "null data1");
        Preconditions.checkArgument(data2 != null, "null data2");
        Preconditions.checkArgument(off1 >= 0 && off1 <= data1.size(), "invalid off1");
        Preconditions.checkArgument(off2 >= 0 && off2 <= data2.size(), "invalid off2");
        int mismatch = Arrays.mismatch(data1.data, data1.min + off1, data1.max, data2.data, data2.min + off2, data2.max);
        if (mismatch < 0) {
            final int len1 = data1.max - (data1.min + off1);
            final int len2 = data2.max - (data2.min + off2);
            mismatch = Math.min(len1, len2);
        }
        return mismatch;
    }

// Public Instance Methods

    /**
     * Create an {@link IntStream} from this instance.
     *
     * @return stream of integer values in the range 0..255
     */
    public IntStream stream() {
        return IntStream.range(this.min, this.max).map(i -> this.data[i] & 0xff);
    }

    /**
     * Create a {@link Reader} input stream from this instance.
     *
     * @return data input stream
     */
    public Reader newReader() {
        return new Reader(this, this.min, this.size());
    }

    /**
     * Create a {@link Reader} input stream from this instance starting at the given offset.
     *
     * <p>
     * Equivalent to: {@code substring(off).newReader()}.
     *
     * @param off starting offset
     * @return data input stream starting at offset {@code off}
     * @throws IndexOutOfBoundsException if {@code off} is out of bounds
     */
    public Reader newReader(int off) {
        final int size = this.size();
        Preconditions.checkArgument(off >= 0 && off <= size, "invalid offset");
        return new Reader(this, this.min + off, size - off);
    }

    /**
     * Return the data in this instance as a string of lowercase hex digits.
     *
     * @return this instance's data in hexadecimal
     * @throws IndexOutOfBoundsException if the resulting string length would exceed {@link Integer#MAX_VALUE}
     */
    public String toHex() {
        return this.toHex(Integer.MAX_VALUE);
    }

    /**
     * Return the data in this instance as a string of lowercase hex digits, truncated if necessary.
     *
     * <p>
     * If there are more than {@code limit} bytes to display, then this will truncate the result and add an ellipsis.
     *
     * @param limit maximum number bytes to display
     * @return up to {@code limit} bytes of this instance's data in hexadecimal
     * @throws IndexOutOfBoundsException if the resulting string length would exceed {@link Integer#MAX_VALUE}
     * @throws IllegalArgumentException if {@code limit} is negative
     */
    public String toHex(int limit) {
        Preconditions.checkArgument(limit >= 0, "invalid limit");
        return this.size() <= limit ?
          this.toHex(this.min, this.max, false) :
          this.toHex(this.min, this.min + limit, true);
    }

    private String toHex(int beginIndex, int endIndex, boolean ellipsis) {
        assert beginIndex >= 0 && endIndex >= beginIndex;
        final int dataLen = endIndex - beginIndex;
        switch (Integer.numberOfLeadingZeros(dataLen)) {
        case 0:
            assert dataLen == 0;
            return "";
        case 1:
            throw new IndexOutOfBoundsException("string would be too long");
        default:
            break;
        }
        final int stringLen = (dataLen * 2) + (ellipsis ? 3 : 0);
        if (stringLen < 0)
            throw new IndexOutOfBoundsException("string would be too long");
        final StringBuilder result = new StringBuilder(stringLen);
        for (int i = beginIndex; i < endIndex; i++) {
            final byte value = this.data[i];
            final char ch1 = Character.forDigit((value >> 4) & 0x0f, 16);
            final char ch2 = Character.forDigit(value & 0x0f, 16);
            result.append(ch1).append(ch2);
        }
        if (ellipsis)
            result.append("...");
        return result.toString();
    }

    /**
     * Obtain the byte at the given index.
     *
     * @param index index into byte data
     * @return the byte at {@code index}
     * @throws IndexOutOfBoundsException if {@code index} is out of bounds
     */
    public byte byteAt(int index) {
        Objects.checkIndex(index, this.max - this.min);
        return this.data[this.min + index];
    }

    /**
     * Obtain the byte at the given index as an unsigned value.
     *
     * @param index index into byte data
     * @return the byte at {@code index} as a integer in the range 0 to 255
     * @throws IndexOutOfBoundsException if {@code index} is out of bounds
     */
    public int ubyteAt(int index) {
        return this.byteAt(index) & 0xff;
    }

    /**
     * Get the number of bytes in this instance.
     *
     * @return number of bytes
     */
    public int size() {
        return this.max - this.min;
    }

    /**
     * Determine whether this instance is empty, i.e., has zero bytes.
     *
     * @return true if empty
     */
    public boolean isEmpty() {
        return this.min == this.max;
    }

    /**
     * Obtain an instance containing a substring of the data contained by this instance.
     *
     * @param beginIndex starting offset (inclusive)
     * @return substring of this instance from {@code beginIndex} to the end of this instance
     * @throws IndexOutOfBoundsException if {@code beginIndex} is out of bounds
     */
    public ByteData substring(int beginIndex) {
        if (beginIndex == 0)
            return this;
        final int size = this.max - this.min;
        if (beginIndex == size)
            return EMPTY;
        Preconditions.checkArgument(beginIndex >= 0 && beginIndex <= size, "index out of range");
        return new ByteData(this.data, this.min + beginIndex, this.max);
    }

    /**
     * Obtain an instance containing a substring of the data contained by this instance.
     *
     * @param beginIndex starting offset (inclusive)
     * @param endIndex ending offset (exclusive)
     * @return substring of this instance from {@code beginIndex} to {@code endIndex}
     * @throws IndexOutOfBoundsException if {@code beginIndex} and/or {@code endIndex} is invalid
     */
    public ByteData substring(int beginIndex, int endIndex) {
        final int size = this.max - this.min;
        if (beginIndex == 0 && endIndex == size)
            return this;
        Objects.checkFromToIndex(beginIndex, endIndex, size);
        if (beginIndex == endIndex)
            return EMPTY;
        return new ByteData(this.data, this.min + beginIndex, this.min + endIndex);
    }

    /**
     * Obtain an instance containing the concatenation of this instance and the given instance.
     *
     * @param next the instance to append
     * @return concatenation of this instance and {@code next}
     * @throws IllegalArgumentException if {@code next} is null
     * @throws IllegalArgumentException if the concatenation would be longer than {@link Integer#MAX_VALUE}
     */
    public ByteData concat(ByteData next) {
        Preconditions.checkArgument(next != null, "null next");
        final int thisSize = this.size();
        if (thisSize == 0)
            return next;
        final int nextSize = next.size();
        if (nextSize == 0)
            return this;
        final ByteData.Writer writer = ByteData.newWriter(thisSize + nextSize);
        writer.write(this);
        writer.write(next);
        return writer.toByteData();
    }

    /**
     * Determine whether this instance has the given prefix.
     *
     * @param prefix prefix data
     * @return true if this instance starts with {@code prefix}
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public boolean startsWith(ByteData prefix) {
        Preconditions.checkArgument(prefix != null, "null prefix");
        final int thisLen = this.size();
        final int prefLen = prefix.size();
        if (thisLen < prefLen)
            return false;
        return Arrays.equals(this.data, this.min, this.min + prefLen, prefix.data, prefix.min, prefix.max);
    }

    /**
     * Determine whether this instance has the given suffix.
     *
     * @param suffix suffix data
     * @return true if this instance starts with {@code suffix}
     * @throws IllegalArgumentException if {@code suffix} is null
     */
    public boolean endsWith(ByteData suffix) {
        Preconditions.checkArgument(suffix != null, "null suffix");
        final int thisLen = this.size();
        final int suffLen = suffix.size();
        if (thisLen < suffLen)
            return false;
        return Arrays.equals(this.data, this.max - suffLen, this.max, suffix.data, suffix.min, suffix.max);
    }

    /**
     * Obtain the data as a {@code byte[]} array.
     *
     * @return {@code byte[]} array containing a copy of this instance's data
     */
    public byte[] toByteArray() {
        final int size = this.max - this.min;
        final byte[] result = new byte[size];
        System.arraycopy(this.data, this.min, result, 0, size);
        return result;
    }

    /**
     * Write the data into a {@code byte[]} array.
     *
     * @param dest destination for data
     * @param off offset into {@code dest} to write
     * @throws IndexOutOfBoundsException if {@code index} is out of bounds
     * @throws IllegalArgumentException if {@code dest} is null
     */
    public void writeTo(byte[] dest, int off) {
        Preconditions.checkArgument(dest != null, "null dest");
        System.arraycopy(this.data, this.min, dest, off, this.size());
    }

    /**
     * Write the data to the given output stream.
     *
     * @param output destination for data
     * @throws IOExeption if an I/O error occurs
     * @throws IllegalArgumentException if {@code output} is null
     */
    public void writeTo(OutputStream output) throws IOException {
        Preconditions.checkArgument(output != null, "null output");

        // A nefarious OutputStream could corrupt the data array, so we copy the data into a transfer buffer before writing
        final byte[] xferBuf = new byte[Math.min(this.max - this.min, 1000)];
        for (int xferLen, off = this.min; (xferLen = Math.min(this.max - off, xferBuf.length)) > 0; off += xferLen) {
            System.arraycopy(this.data, off, xferBuf, 0, xferLen);
            output.write(xferBuf, 0, xferLen);
        }
    }

    /**
     * Write the data into the given byte buffer at its current position (relative write).
     *
     * @param buf destination for data
     * @throws IndexOutOfBoundsException if the data goes out of bounds
     * @throws IllegalArgumentException if {@code buf} is null
     */
    public void writeTo(ByteBuffer buf) {
        Preconditions.checkArgument(buf != null, "null buf");
        buf.put(this.data, this.min, this.size());
    }

    /**
     * Write the data into the given byte buffer at the specified position (absolute write).
     *
     * @param buf destination for data
     * @param index absolute index in {@code buf} at which to write the data
     * @throws IndexOutOfBoundsException if the data goes out of bounds
     * @throws IllegalArgumentException if {@code buf} is null
     */
    public void writeTo(ByteBuffer buf, int index) {
        Preconditions.checkArgument(buf != null, "null buf");
        buf.put(index, this.data, this.min, this.size());
    }

// Comparable

    /**
     * Compare this instance with the given instance using using unsigned lexicographical comparison.
     *
     * @throws NullPointerException if {@code that} is null
     */
    @Override
    public int compareTo(ByteData that) {
        return Arrays.compareUnsigned(this.data, this.min, this.max, that.data, that.min, that.max);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ByteData that = (ByteData)obj;
        final int thisLen = this.size();
        final int thatLen = that.size();
        if (thisLen != thatLen)
            return false;
        return Arrays.equals(this.data, this.min, this.max, that.data, that.min, that.max);
    }

    @Override
    public int hashCode() {
        if (this.hash == 0) {
            if (this.min == 0 && this.max == this.data.length)
                return Arrays.hashCode(this.data);
            int value = 1;
            for (int i = this.min; i < this.max; i++)
                value = value * 31 + this.data[i];
            this.hash = value;
        }
        return this.hash;
    }

    @Override
    public String toString() {
        return String.format("ByteData[size=%d,data=%s]", this.size(), this.toHex(MAX_TOSTRING_HEXBYTES));
    }

// Output

    /**
     * Gathers data used to build a {@link ByteData} instance.
     *
     * <p>
     * If any write operation would cause the total size to exceed {@link Integer#MAX_VALUE}, then
     * an {@link IndexOutOfBoundsException} is thrown.
     *
     * <p>
     * Instances are thread safe.
     */
    public static final class Writer extends OutputStream {

        private static final int DEFAULT_INITIAL_CAPACITY = 128;

        private byte[] buf;         // data buffer
        private int size;           // the number of valid bytes in "buf" so far
        private int numReadOnly;    // the number of bytes in "buf" referenced by some ByteData instance and therefore read-only

        private Writer() {
            this(DEFAULT_INITIAL_CAPACITY);
        }

        private Writer(int initialCapacity) {
            Preconditions.checkArgument(initialCapacity >= 0, "negative initialCapacity");
            synchronized (this) {
                this.buf = new byte[initialCapacity];
            }
        }

    // OutputStream

        @Override
        public synchronized void write(int b) {
            this.makeRoom(1);
            this.buf[this.size++] = (byte)b;
        }

        @Override
        public void write(byte[] data) {
            this.write(data, 0, data.length);
        }

        @Override
        public synchronized void write(byte[] data, int off, int len) {
            Objects.checkFromIndexSize(off, len, data.length);
            this.makeRoom(len);
            System.arraycopy(data, off, this.buf, this.size, len);
            this.size += len;
        }

    // Other Methods

        /**
         * Write the given byte data to this instance.
         *
         * @param data data to write
         * @throws IllegalArgumentException if {@code data} is null
         */
        public synchronized void write(ByteData data) {
            Preconditions.checkArgument(data != null, "null data");
            final int len = data.size();
            this.makeRoom(len);
            System.arraycopy(data.data, data.min, this.buf, this.size, len);
            this.size += len;
        }

        /**
         * Return a {@link ByteData} containing the data written to this instance.
         *
         * @return the data written to this writer
         */
        public synchronized ByteData toByteData() {

            // Minor optimization
            if (this.size == 0)
                return ByteData.EMPTY;

            // Build result
            final ByteData result = new ByteData(this.buf, 0, this.size);

            // Mark the first "size" bytes as read-only, unless the entire buffer
            // was utilized, in which case there's no point in keeping it around.
            if (this.size < this.buf.length)
                this.numReadOnly = Math.max(this.numReadOnly, this.size);
            else {
                this.buf = ByteData.EMPTY_BYTE_ARRAY;       // probably this will never get used, so keep it short
                this.numReadOnly = 0;
            }

            // Done
            return result;
        }

        /**
         * Get the number of bytes written to this instance so far.
         *
         * @return size of this instance
         */
        public synchronized int size() {
            return this.size;
        }

        /**
         * Reset this instance so that it contains zero bytes.
         */
        public void reset() {
            this.truncate(0);
        }

        /**
         * Truncate this instance to discard all but the first {@link size} bytes previously written.
         *
         * @param size new truncated size
         * @throws IndexOutOfBoundsException if {@code size} is negative or greater than this instance's current size
         */
        public synchronized void truncate(int size) {
            Preconditions.checkArgument(size >= 0 && size <= this.size, "size out of range");
            this.size = size;
        }

    // Internal Methods

        /**
         * Expand the internal buffer as needed to ensure there is room to add the specified number of new bytes.
         *
         * @param len number of additional bytes to make room for
         * @throws IndexOutOfBoundsException if {@code len} is negative or would cause the buffer
         *  to exceed {@link Integer#MAX_VALUE} bytes
         */
        public synchronized void makeRoom(int len) {

            // Sanity check
            if (len <= 0) {
                if (len < 0)
                    throw new IndexOutOfBoundsException("negative length");
                return;
            }

            // Calculate the new minimum required size of the buffer due to the new data
            final int minNewLength = this.size + len;
            if (minNewLength < 0)
                throw new IndexOutOfBoundsException("maximum buffer capacity exceeded");

            // Allocate a new buffer if (a) the current one is too short or (b) the next byte is read-only
            if (this.buf.length < minNewLength || this.numReadOnly > this.size) {

                // Increase buffer size at least exponentially to avoid O(n²) behavior due to copying
                int minNewLength2 = Math.max(this.size, 16) * 2;
                if (minNewLength2 < 0)
                    minNewLength2 = Integer.MAX_VALUE;

                // Determine the new buffer size
                final int newLength = Math.max(minNewLength, minNewLength2);

                // Allocate the new buffer and replace existing
                final byte[] newBuf = new byte[newLength];
                System.arraycopy(this.buf, 0, newBuf, 0, this.size);
                this.buf = newBuf;
                this.numReadOnly = 0;
            }
        }
    }

// Reader

    /**
     * Reads out the data from an underlying {@link ByteData} instance.
     *
     * <p>
     * Instances are thread safe and fully support {@link InputStream#available} and mark/reset.
     */
    public static final class Reader extends ByteArrayInputStream {

        private final ByteData data;

        private Reader(ByteData data, int off, int len) {
            super(data.data, off, len);
            assert off >= 0 && len >= 0 && off + len >= 0 && off + len <= data.data.length;
            this.data = data;
        }

        /**
         * Peek at the next byte, if any.
         *
         * <p>
         * This does not change the current read offset.
         *
         * @return next byte (0-255)
         * @throws IndexOutOfBoundsException if there are no more bytes
         */
        public synchronized int peek() {
            if (this.pos >= this.count)
                throw new IndexOutOfBoundsException("truncated input");
            return this.buf[this.pos] & 0xff;
        }

        /**
         * Read the next byte as an unsigned value.
         *
         * @return next byte (0-255)
         * @throws IndexOutOfBoundsException if there are no more bytes
         */
        public synchronized int readByte() {
            if (this.pos >= this.count)
                throw new IndexOutOfBoundsException("truncated input");
            return this.buf[this.pos++] & 0xff;
        }

        /**
         * Unread the previously read byte.
         *
         * <p>
         * Equivalent to {@code unread(1)}.
         *
         * @throws IndexOutOfBoundsException if zero bytes have been read
         */
        public void unread() {
            this.unread(1);
        }

        /**
         * Unread the specified number of previously read bytes.
         *
         * <p>
         * Upon return this instance's current offset will be decremented by {@code len} bytes.
         *
         * @param len the number of bytes to unread
         * @throws IndexOutOfBoundsException if {@code len} is negative or greater than the number of bytes that have been read
         */
        public synchronized void unread(int len) {
            final int maxLen = this.pos - this.data.min;
            if (len < 0 || len > maxLen)
                throw new IndexOutOfBoundsException(String.format("invalid length %d not in the range 0..%d", len, maxLen));
            this.pos -= len;
        }

        /**
         * Get the number of bytes remaining.
         *
         * @return bytes remaining
         */
        public synchronized int remain() {
            return this.count - this.pos;
        }

        /**
         * Get the current offset into the underlying data.
         *
         * @return current offset
         */
        public synchronized int getOffset() {
            return this.pos - this.data.min;
        }

        /**
         * Read out the specified number of bytes.
         *
         * @param len number of bytes to read
         * @return bytes read
         * @throws IndexOutOfBoundsException if {@code len} is negative or greater than the number of bytes remaining
         */
        public synchronized ByteData readBytes(int len) {
            final int offset = this.pos - this.data.min;
            final ByteData result;
            try {
                result = this.data.substring(offset, offset + len);
            } catch (IndexOutOfBoundsException e) {
                final int maxLen = this.count - this.pos;
                throw new IndexOutOfBoundsException(String.format("invalid length %d not in the range 0..%d", len, maxLen));
            }
            this.pos += len;
            return result;
        }

        /**
         * Obtain a {@link ByteData} instance containing all of the data read from this instance so far.
         *
         * <p>
         * This does not change the current read offset.
         *
         * @return all data read so far
         */
        public synchronized ByteData dataReadSoFar() {
            return this.data.substring(0, this.pos - this.data.min);
        }

        /**
         * Obtain a {@link ByteData} instance containing all of the data read not yet read from this instance.
         *
         * <p>
         * This does not change the current read offset.
         *
         * @return all unread data
         */
        public synchronized ByteData dataNotYetRead() {
            return this.data.substring(this.pos - this.data.min);
        }

        /**
         * Read out all remaining bytes.
         *
         * <p>
         * Upon return this instance's current offset will be positioned at the end of the data.
         *
         * @return bytes read
         */
        public synchronized ByteData readRemaining() {
            final ByteData result = this.dataNotYetRead();
            this.pos = this.count;
            return result;
        }

        /**
         * Obtain the {@link ByteData} instance underlying this reader.
         *
         * <p>
         * This does not change the current read offset.
         *
         * @return copy of the entire buffer
         */
        public ByteData getByteData() {
            return this.data;
        }
    }
}
