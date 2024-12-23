
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.OptionalInt;

/**
 * Non-null {@link LocalDateTime} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via two consecutive {@link LongEncoder}-encoded values, {@linkplain Instant#getEpochSecond epoch seconds}
 * followed by {@linkplain Instant#getNano nanoseconds} of the date/time in {@link ZoneOffset#UTC UTC}.
 */
public class LocalDateTimeEncoding extends AbstractEncoding<LocalDateTime> {

    private static final long serialVersionUID = -3302238853808401737L;

    public LocalDateTimeEncoding() {
        super(LocalDateTime.class);
    }

// Encoding

    @Override
    public LocalDateTime read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return LocalDateTime.ofEpochSecond(LongEncoder.read(reader), (int)LongEncoder.read(reader), ZoneOffset.UTC);
    }

    @Override
    public void write(ByteData.Writer writer, LocalDateTime localDateTime) {
        Preconditions.checkArgument(localDateTime != null, "null localDateTime");
        Preconditions.checkArgument(writer != null);
        final Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
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
    public LocalDateTime fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return LocalDateTime.parse(string);
    }

    @Override
    public String toString(LocalDateTime localDateTime) {
        Preconditions.checkArgument(localDateTime != null, "null localDateTime");
        return localDateTime.toString();
    }

    @Override
    public int compare(LocalDateTime localDateTime1, LocalDateTime localDateTime2) {
        return localDateTime1.compareTo(localDateTime2);
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
