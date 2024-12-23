
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import java.util.OptionalInt;

import org.dellroad.stuff.java.Primitive;

public class BooleanEncoding extends PrimitiveEncoding<Boolean> {

    private static final long serialVersionUID = 5941222137600409101L;

    private static final byte FALSE_VALUE = (byte)0;
    private static final byte TRUE_VALUE = (byte)1;

    public BooleanEncoding(EncodingId encodingId) {
       super(encodingId, Primitive.BOOLEAN);
    }

    @Override
    public Boolean read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final int value = reader.readByte();
        switch (value) {
        case FALSE_VALUE:
            return Boolean.FALSE;
        case TRUE_VALUE:
            return Boolean.TRUE;
        default:
            throw new IllegalArgumentException(String.format("invalid encoded boolean value 0x%02x", value));
        }
    }

    @Override
    public void write(ByteData.Writer writer, Boolean value) {
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(value != null, "null value");
        writer.write(value ? TRUE_VALUE : FALSE_VALUE);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(1);
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
    public OptionalInt getFixedWidth() {
        return OptionalInt.of(1);
    }

// Conversion

    @Override
    protected Boolean convertNumber(Number value) {
        return value.doubleValue() != 0;
    }
}
