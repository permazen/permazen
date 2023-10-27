
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.core.Encoding;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;
import io.permazen.util.UnsignedIntEncoder;

import org.dellroad.stuff.java.Primitive;

/**
 * Encoding for unsigned {@code int} values in the manner of {@link UnsignedIntEncoder}.
 *
 * <p>
 * This type is internally for encoding various non-negative integer values.
 *
 * <p>
 * Instances are {@linkplain Encoding#getEncodingId anonymous}.
 */
public class UnsignedIntType extends NonNullEncoding<Integer> {

    private static final long serialVersionUID = 4653435311425384497L;

    public UnsignedIntType() {
        super(null, Integer.class, 0);
    }

    @Override
    public Integer read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return UnsignedIntEncoder.read(reader);
    }

    @Override
    public void write(ByteWriter writer, Integer value) {
        Preconditions.checkArgument(value != null, "null value");
        Preconditions.checkArgument(writer != null);
        UnsignedIntEncoder.write(writer, value);
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(UnsignedIntEncoder.decodeLength(reader.peek()));
    }

    @Override
    public String toParseableString(Integer value) {
        Preconditions.checkArgument(value != null, "null value");
        return String.valueOf(value);
    }

    @Override
    public Integer fromParseableString(ParseContext ctx) {
        return Primitive.INTEGER.parseValue(ctx.matchPrefix(Primitive.INTEGER.getParsePattern()).group());
    }

    @Override
    public int compare(Integer value1, Integer value2) {
        return Integer.compare(value1, value2);
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }
}
