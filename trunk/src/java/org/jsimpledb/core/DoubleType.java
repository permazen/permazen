
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link Double} type.
 */
class DoubleType extends PrimitiveType<Double> {

    private static final byte[] DEFAULT_VALUE = new byte[] {
      (byte)0x80, (byte)0x00, (byte)0x00, (byte)0x00,
      (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00
    };

    private static final long POS_XOR = 0x8000000000000000L;
    private static final long NEG_XOR = 0xffffffffffffffffL;
    private static final long SIGN_BIT = 0x8000000000000000L;

    DoubleType() {
       super(Primitive.DOUBLE);
    }

    @Override
    public Double read(ByteReader reader) {
        long bits =
            ((long)reader.readByte() << 56)
          | ((long)reader.readByte() << 48)
          | ((long)reader.readByte() << 40)
          | ((long)reader.readByte() << 32)
          | ((long)reader.readByte() << 24)
          | ((long)reader.readByte() << 16)
          | ((long)reader.readByte() <<  8)
          | ((long)reader.readByte());
        bits ^= (bits & SIGN_BIT) == 0 ? NEG_XOR : POS_XOR;
        return Double.longBitsToDouble(bits);
    }

    @Override
    public void write(ByteWriter writer, Double value) {
        long bits = Double.doubleToLongBits(value);
        bits ^= (bits & SIGN_BIT) != 0 ? NEG_XOR : POS_XOR;
        writer.writeByte((int)(bits >> 56));
        writer.writeByte((int)(bits >> 48));
        writer.writeByte((int)(bits >> 40));
        writer.writeByte((int)(bits >> 32));
        writer.writeByte((int)(bits >> 24));
        writer.writeByte((int)(bits >> 16));
        writer.writeByte((int)(bits >> 8));
        writer.writeByte((int)bits);
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(8);
    }

    @Override
    public byte[] getDefaultValue() {
        return DEFAULT_VALUE;
    }
}

