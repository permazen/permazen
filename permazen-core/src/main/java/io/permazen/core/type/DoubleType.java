
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
 * {@link Double} type.
 */
public class DoubleType extends NumberType<Double> {

    private static final long serialVersionUID = 7124114664265270273L;

    private static final long POS_XOR = 0x8000000000000000L;
    private static final long NEG_XOR = 0xffffffffffffffffL;
    private static final long SIGN_BIT = 0x8000000000000000L;

    public DoubleType() {
       super(Primitive.DOUBLE);
    }

    @Override
    public Double read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        long bits = ByteUtil.readLong(reader);
        bits ^= (bits & SIGN_BIT) == 0 ? NEG_XOR : POS_XOR;
        return Double.longBitsToDouble(bits);
    }

    @Override
    public void write(ByteWriter writer, Double value) {
        Preconditions.checkArgument(writer != null);
        long bits = Double.doubleToLongBits(value);
        bits ^= (bits & SIGN_BIT) != 0 ? NEG_XOR : POS_XOR;
        ByteUtil.writeLong(writer, bits);
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(8);
    }

    @Override
    public Double validate(Object obj) {
        if (obj instanceof Character)
            return (double)(Character)obj;
        if (obj instanceof Byte || obj instanceof Short
          || obj instanceof Integer || obj instanceof Float || obj instanceof Long)
            return ((Number)obj).doubleValue();
        return super.validate(obj);
    }

// Conversion

    @Override
    protected Double convertNumber(Number value) {
        return value.doubleValue();
    }
}

