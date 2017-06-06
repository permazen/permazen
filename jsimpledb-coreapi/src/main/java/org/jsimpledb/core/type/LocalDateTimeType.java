
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Pattern;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link LocalDateTime} type. Null values are not supported by this class.
 */
public class LocalDateTimeType extends NonNullFieldType<LocalDateTime> {

    static final Pattern PATTERN = Pattern.compile("-?[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+(:[0-9]+(\\.[0-9]+)?)?");

    private static final long serialVersionUID = -3302238853808401737L;

    public LocalDateTimeType() {
        super(LocalDateTime.class, 0);
    }

// FieldType

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
        return LocalDateTime.parse(ctx.matchPrefix(LocalDateTimeType.PATTERN).group());
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
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }
}

