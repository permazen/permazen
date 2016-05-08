
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

class BooleanType extends PrimitiveType<Boolean> {

    private static final byte FALSE_VALUE = (byte)0;
    private static final byte TRUE_VALUE = (byte)1;

    BooleanType() {
       super(Primitive.BOOLEAN);
    }

    @Override
    public Boolean read(ByteReader reader) {
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
    public void write(ByteWriter writer, Boolean value) {
        Preconditions.checkArgument(value != null, "null value");
        writer.writeByte(value ? TRUE_VALUE : FALSE_VALUE);
    }

    @Override
    public void skip(ByteReader reader) {
        this.read(reader);
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }
}

