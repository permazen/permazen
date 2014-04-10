
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link Float} type.
 */
class FloatType extends PrimitiveType<Float> {

    private static final byte[] DEFAULT_VALUE = new byte[] {
      (byte)0x80, (byte)0x00, (byte)0x00, (byte)0x00
    };

    private static final int POS_XOR = 0x80000000;
    private static final int NEG_XOR = 0xffffffff;
    private static final int SIGN_BIT = 0x80000000;

    FloatType() {
       super(Primitive.FLOAT);
    }

    @Override
    public Float read(ByteReader reader) {
        int bits =
            (reader.readByte() << 24)
          | (reader.readByte() << 16)
          | (reader.readByte() <<  8)
          | (reader.readByte());
        bits ^= (bits & SIGN_BIT) == 0 ? NEG_XOR : POS_XOR;
        return Float.intBitsToFloat(bits);
    }

    @Override
    public void write(ByteWriter writer, Float value) {
        int bits = Float.floatToIntBits(value);
        bits ^= (bits & SIGN_BIT) != 0 ? NEG_XOR : POS_XOR;
        writer.writeByte(bits >> 24);
        writer.writeByte(bits >> 16);
        writer.writeByte(bits >> 8);
        writer.writeByte(bits);
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        writer.write(reader.readBytes(4));
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(4);
    }

    @Override
    public byte[] getDefaultValue() {
        return DEFAULT_VALUE;
    }
}

