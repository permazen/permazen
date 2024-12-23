
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Booleans;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;

import java.util.BitSet;
import java.util.List;

/**
 * Non-null {@code boolean[]} array type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Each boolean value is encoded in two bits: end of array ({@code 00}), false ({@code 01}), or true ({@code 10}).
 * Four bit pairs are stored in each encoded byte, starting with the high-order bit pair.
 */
public class BooleanArrayEncoding extends ArrayEncoding<boolean[], Boolean> {

    private static final long serialVersionUID = -7254445869697946454L;

    private static final int END = 0x00;
    private static final int FALSE = 0x01;
    private static final int TRUE = 0x02;

    @SuppressWarnings("serial")
    public BooleanArrayEncoding() {
        super(new BooleanEncoding(null), new TypeToken<boolean[]>() { });
    }

    @Override
    public boolean[] read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final BitSet bits = new BitSet();
        int count = 0;
loop:   while (true) {
            int value = reader.readByte();
            for (int shift = 6; shift >= 0; shift -= 2) {
                switch ((value >> shift) & 0x03) {
                case END:
                    break loop;
                case FALSE:
                    bits.clear(count++);
                    break;
                case TRUE:
                    bits.set(count++);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("invalid encoding of %s", this));
                }
            }
        }
        final boolean[] result = new boolean[count];
        for (int i = 0; i < count; i++)
            result[i] = bits.get(i);
        return result;
    }

    @Override
    public void write(ByteData.Writer writer, boolean[] array) {
        Preconditions.checkArgument(writer != null);
        int value = 0;
        for (int i = 0; i < array.length; i++) {
            final int phase = i % 4;
            final int shift = 2 * (3 - phase);
            value |= (array[i] ? TRUE : FALSE) << shift;
            if (phase == 3) {
                writer.write(value);
                value = 0;
            }
        }
        writer.write(value);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        while (true) {
            int value = reader.readByte();
            if ((value & 0xc0) == 0
              || (value & 0x30) == 0
              || (value & 0x0c) == 0
              || (value & 0x03) == 0)
                break;
        }
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    protected int getArrayLength(boolean[] array) {
        return array.length;
    }

    @Override
    protected Boolean getArrayElement(boolean[] array, int index) {
        return array[index];
    }

    @Override
    protected boolean[] createArray(List<Boolean> elements) {
        return Booleans.toArray(elements);
    }
}
