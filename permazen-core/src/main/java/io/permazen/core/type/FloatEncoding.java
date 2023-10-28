
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import org.dellroad.stuff.java.Primitive;

/**
 * {@link Float} type.
 */
public class FloatEncoding extends NumberEncoding<Float> {

    private static final long serialVersionUID = 4726406311612739536L;

    private static final byte[] DEFAULT_VALUE = new byte[] {
      (byte)0x80, (byte)0x00, (byte)0x00, (byte)0x00
    };

    private static final int POS_XOR = 0x80000000;
    private static final int NEG_XOR = 0xffffffff;
    private static final int SIGN_BIT = 0x80000000;

    public FloatEncoding() {
       super(Primitive.FLOAT);
    }

    @Override
    public Float read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        int bits = ByteUtil.readInt(reader);
        bits ^= (bits & SIGN_BIT) == 0 ? NEG_XOR : POS_XOR;
        return Float.intBitsToFloat(bits);
    }

    @Override
    public void write(ByteWriter writer, Float value) {
        Preconditions.checkArgument(writer != null);
        int bits = Float.floatToIntBits(value);
        bits ^= (bits & SIGN_BIT) != 0 ? NEG_XOR : POS_XOR;
        ByteUtil.writeInt(writer, bits);
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(4);
    }

    @Override
    public Float validate(Object obj) {
        if (obj instanceof Character)
            return (float)(Character)obj;
        if (obj instanceof Byte || obj instanceof Short || obj instanceof Integer || obj instanceof Long)
            return ((Number)obj).floatValue();
        return super.validate(obj);
    }

// Conversion

    @Override
    protected Float convertNumber(Number value) {
        return value.floatValue();
    }
}
