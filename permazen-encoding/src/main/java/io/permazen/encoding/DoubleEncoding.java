
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.OptionalInt;

import org.dellroad.stuff.java.Primitive;

/**
 * {@link Double} type.
 */
public class DoubleEncoding extends NumberEncoding<Double> {

    private static final long serialVersionUID = 7124114664265270273L;

    private static final long POS_XOR = 0x8000000000000000L;
    private static final long NEG_XOR = 0xffffffffffffffffL;
    private static final long SIGN_BIT = 0x8000000000000000L;

    public DoubleEncoding(EncodingId encodingId) {
       super(encodingId, Primitive.DOUBLE);
    }

    @Override
    public Double read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        long bits = ByteUtil.readLong(reader);
        bits ^= (bits & SIGN_BIT) == 0 ? NEG_XOR : POS_XOR;
        return Double.longBitsToDouble(bits);
    }

    @Override
    public void write(ByteData.Writer writer, Double value) {
        Preconditions.checkArgument(writer != null);
        long bits = Double.doubleToLongBits(value);
        bits ^= (bits & SIGN_BIT) != 0 ? NEG_XOR : POS_XOR;
        ByteUtil.writeLong(writer, bits);
    }

    @Override
    public void skip(ByteData.Reader reader) {
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

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return true;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.of(8);
    }

// Conversion

    @Override
    protected Double convertNumber(Number value) {
        return value.doubleValue();
    }
}
