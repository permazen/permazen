
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.util.OptionalInt;

import org.dellroad.stuff.java.Primitive;

/**
 * Non-null encoding for unsigned {@code int} values in the manner of {@link UnsignedIntEncoder}.
 *
 * <p>
 * This type is internally for encoding various non-negative integer values.
 */
public class UnsignedIntEncoding extends AbstractEncoding<Integer> {

    private static final long serialVersionUID = 4653435311425384497L;

// Constructors

    /**
     * Constructor.
     */
    public UnsignedIntEncoding() {
        super(Integer.class);
    }

// Encoding

    @Override
    public Integer read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return UnsignedIntEncoder.read(reader);
    }

    @Override
    public void write(ByteData.Writer writer, Integer value) {
        Preconditions.checkArgument(value != null, "null value");
        Preconditions.checkArgument(writer != null);
        UnsignedIntEncoder.write(writer, value);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(UnsignedIntEncoder.decodeLength(reader.peek()));
    }

    @Override
    public String toString(Integer value) {
        Preconditions.checkArgument(value != null, "null value");
        return String.valueOf(value);
    }

    @Override
    public Integer fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return Primitive.INTEGER.parseValue(string);
    }

    @Override
    public int compare(Integer value1, Integer value2) {
        return Integer.compare(value1, value2);
    }

    @Override
    public boolean supportsNull() {
        return false;
    }

    @Override
    public boolean sortsNaturally() {
        return true;
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
        return OptionalInt.empty();
    }
}
