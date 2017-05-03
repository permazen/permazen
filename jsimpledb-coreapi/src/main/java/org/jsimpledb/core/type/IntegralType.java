
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;

/**
 * Support superclass for the integral types encoded via {@link LongEncoder}.
 */
public abstract class IntegralType<T extends Number> extends NumberType<T> {

    private static final long serialVersionUID = -4654999812179346709L;

    protected IntegralType(Primitive<T> primitive) {
       super(primitive);
    }

    @Override
    public T read(ByteReader reader) {
        return this.downCast(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, T value) {
        Preconditions.checkArgument(value != null, "null value");
        LongEncoder.write(writer, this.upCast(value));
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    protected abstract T downCast(long value);

    protected long upCast(T value) {
        return value.longValue();
    }
}

