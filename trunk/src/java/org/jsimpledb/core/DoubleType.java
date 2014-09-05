
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
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
        long bits = ByteUtil.readLong(reader);
        bits ^= (bits & SIGN_BIT) == 0 ? NEG_XOR : POS_XOR;
        return Double.longBitsToDouble(bits);
    }

    @Override
    public void write(ByteWriter writer, Double value) {
        long bits = Double.doubleToLongBits(value);
        bits ^= (bits & SIGN_BIT) != 0 ? NEG_XOR : POS_XOR;
        ByteUtil.writeLong(writer, bits);
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(8);
    }

    @Override
    public byte[] getDefaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public Double validate(Object obj) {
        if (obj instanceof Character)
            return (double)((Character)obj).charValue();
        if (obj instanceof Byte || obj instanceof Short
          || obj instanceof Integer || obj instanceof Float || obj instanceof Long)
            return ((Number)obj).doubleValue();
        return super.validate(obj);
    }
}

