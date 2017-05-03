
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.ParseContext;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Field type for encoding unsigned {@code int} values in the manner of {@link UnsignedIntEncoder}.
 *
 * <p>
 * This type is internally for encoding various non-negative integer values.
 */
public class UnsignedIntType extends NonNullFieldType<Integer> {

    private static final long serialVersionUID = 4653435311425384497L;

    public UnsignedIntType() {
        super("uint", TypeToken.of(Integer.class), 0, 0);
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

