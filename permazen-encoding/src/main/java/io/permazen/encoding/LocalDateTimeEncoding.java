
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;
import io.permazen.util.ParseContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Pattern;

/**
 * Non-null {@link LocalDateTime} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via two consecutive {@link LongEncoder}-encoded values, {@linkplain Instant#getEpochSecond epoch seconds}
 * followed by {@linkplain Instant#getNano nanoseconds} of the date/time in {@link ZoneOffset#UTC UTC}.
 */
public class LocalDateTimeEncoding extends BuiltinEncoding<LocalDateTime> {

    static final Pattern PATTERN = Pattern.compile("-?[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+(:[0-9]+(\\.[0-9]+)?)?");

    private static final long serialVersionUID = -3302238853808401737L;

    public LocalDateTimeEncoding() {
        super(LocalDateTime.class);
    }

// Encoding

    @Override
    public LocalDateTime read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return LocalDateTime.ofEpochSecond(LongEncoder.read(reader), (int)LongEncoder.read(reader), ZoneOffset.UTC);
    }

    @Override
    public void write(ByteWriter writer, LocalDateTime localDateTime) {
        Preconditions.checkArgument(localDateTime != null, "null localDateTime");
        Preconditions.checkArgument(writer != null);
        final Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
        LongEncoder.write(writer, instant.getEpochSecond());
        LongEncoder.write(writer, instant.getNano());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public LocalDateTime fromParseableString(ParseContext ctx) {
        return LocalDateTime.parse(ctx.matchPrefix(LocalDateTimeEncoding.PATTERN).group());
    }

    @Override
    public String toParseableString(LocalDateTime localDateTime) {
        return localDateTime.toString();
    }

    @Override
    public int compare(LocalDateTime localDateTime1, LocalDateTime localDateTime2) {
        return localDateTime1.compareTo(localDateTime2);
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
}
