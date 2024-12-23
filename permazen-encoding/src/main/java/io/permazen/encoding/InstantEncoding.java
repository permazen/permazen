
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.time.Instant;
import java.util.OptionalInt;

/**
 * Non-null {@link Instant} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via two consecutive {@link LongEncoder}-encoded values, {@linkplain Instant#getEpochSecond epoch seconds}
 * followed by {@linkplain Instant#getNano nanoseconds}.
 */
public class InstantEncoding extends AbstractEncoding<Instant> {

    private static final long serialVersionUID = -3907615112193058091L;

    public InstantEncoding() {
        super(Instant.class);
    }

// Encoding

    @Override
    public Instant read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return Instant.ofEpochSecond(LongEncoder.read(reader), (int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, Instant instant) {
        Preconditions.checkArgument(instant != null, "null instant");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, instant.getEpochSecond());
        LongEncoder.write(writer, instant.getNano());
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Instant fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return Instant.parse(string);
    }

    @Override
    public String toString(Instant instant) {
        Preconditions.checkArgument(instant != null, "null instant");
        return instant.toString();
    }

    @Override
    public int compare(Instant instant1, Instant instant2) {
        return instant1.compareTo(instant2);
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
}
