
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;

/**
 * Utility methods.
 */
public final class Util {

    private Util() {
    }

    /**
     * Convert {@link ByteData} to {@link ByteIterable}.
     *
     * @param data input {@link ByteData}, or null
     * @return equivalent {@link ByteIterable}, or null if {@code data} is null
     */
    public static ByteIterable wrap(ByteData data) {
        return data != null ? new ByteDataIterable(data) : null;
    }

    /**
     * Convert {@link ByteIterable} to {@link ByteData}.
     *
     * @param data input {@link ByteIterable}, or null
     * @return equivalent {@link ByteData}, or null if {@code data} is null
     */
    public static ByteData unwrap(ByteIterable data) {
        return data != null ? ByteData.of(data.getBytesUnsafe(), 0, data.getLength()) : null;
    }

// ByteDataIterable

    /**
     * A {@link ByteIterable} view of a {@link ByteData}.
     */
    public static class ByteDataIterable implements ByteIterable {

        private final ByteData data;

        public ByteDataIterable(ByteData data) {
            Preconditions.checkArgument(data != null, "null data");
            this.data = data;
        }

        @Override
        public byte[] getBytesUnsafe() {
            return this.data.toByteArray();
        }

        @Override
        public int getLength() {
            return this.data.size();
        }

        @Override
        public ByteIterator iterator() {
            return new ByteDataIterator(this.data.newReader());
        }

        @Override
        public ByteIterable subIterable(int offset, int length) {
            return new ByteDataIterable(this.data.substring(offset, offset + length));
        }

    // Comparable

        @Override
        public int compareTo(ByteIterable that) {
            final ByteIterator i1 = this.iterator();
            final ByteIterator i2 = that.iterator();
            while (true) {
                final boolean next1 = i1.hasNext();
                final boolean next2 = i2.hasNext();
                if (next1 != next2)
                    return next1 ? 1 : -1;
                if (!next1)
                    return 0;
                final int diff = (i1.next() & 0xff) - (i2.next() & 0xff);
                if (diff != 0)
                    return diff;
            }
        }

    // Object

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final ByteDataIterable that = (ByteDataIterable)obj;
            return this.data.equals(that.data);
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode()
              ^ this.data.hashCode();
        }
    }

// ByteDataIterator

    /**
     * A {@link ByteIterator} view of a {@link ByteData.Reader}.
     */
    public static class ByteDataIterator extends ByteIterator {

        private final ByteData.Reader reader;

        public ByteDataIterator(ByteData.Reader reader) {
            Preconditions.checkArgument(reader != null, "null reader");
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            return this.reader.remain() > 0;
        }

        @Override
        public byte next() {
            return (byte)this.reader.readByte();
        }

        @Override
        public long skip(long bytes) {
            return this.reader.skip(bytes);
        }
    }
}
