
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;

import org.dellroad.stuff.java.Primitive;

/**
 * Support superclass for the integral types encoded via {@link LongEncoder}.
 */
public abstract class IntegralEncoding<T extends Number> extends NumberEncoding<T> {

    private static final long serialVersionUID = -4654999812179346709L;

    protected IntegralEncoding(Primitive<T> primitive) {
       super(primitive);
    }

    @Override
    public T read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return this.downCast(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, T value) {
        Preconditions.checkArgument(value != null, "null value");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, this.upCast(value));
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
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
