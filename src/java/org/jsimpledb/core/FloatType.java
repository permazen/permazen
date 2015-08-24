
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
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
        int bits = ByteUtil.readInt(reader);
        bits ^= (bits & SIGN_BIT) == 0 ? NEG_XOR : POS_XOR;
        return Float.intBitsToFloat(bits);
    }

    @Override
    public void write(ByteWriter writer, Float value) {
        int bits = Float.floatToIntBits(value);
        bits ^= (bits & SIGN_BIT) != 0 ? NEG_XOR : POS_XOR;
        ByteUtil.writeInt(writer, bits);
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(4);
    }

    @Override
    public Float validate(Object obj) {
        if (obj instanceof Character)
            return (float)((Character)obj).charValue();
        if (obj instanceof Byte || obj instanceof Short || obj instanceof Integer || obj instanceof Long)
            return ((Number)obj).floatValue();
        return super.validate(obj);
    }
}

