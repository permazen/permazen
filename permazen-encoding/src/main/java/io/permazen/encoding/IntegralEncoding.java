
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.util.OptionalInt;

import org.dellroad.stuff.java.Primitive;

/**
 * Support superclass for the integral types encoded via {@link LongEncoder}.
 */
public abstract class IntegralEncoding<T extends Number> extends NumberEncoding<T> {

    private static final long serialVersionUID = -4654999812179346709L;

    protected IntegralEncoding(EncodingId encodingId, Primitive<T> primitive) {
       super(encodingId, primitive);
    }

    @Override
    public T read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return this.downCast(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, T value) {
        Preconditions.checkArgument(value != null, "null value");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, this.upCast(value));
    }

    @Override
    public void skip(ByteData.Reader reader) {
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

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.empty();
    }

    protected abstract T downCast(long value);

    protected long upCast(T value) {
        return value.longValue();
    }
}
