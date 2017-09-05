
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import java.time.LocalTime;
import java.util.regex.Pattern;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;
import io.permazen.util.ParseContext;

/**
 * Non-null {@link LocalTime} type. Null values are not supported by this class.
 */
public class LocalTimeType extends NonNullFieldType<LocalTime> {

    static final Pattern PATTERN = Pattern.compile("[0-9]+:[0-9]+(:[0-9]+(\\.[0-9]+)?)?");

    private static final long serialVersionUID = -6138317689607411426L;

    public LocalTimeType() {
        super(LocalTime.class, 0);
    }

// FieldType

    @Override
    public LocalTime read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return LocalTime.ofNanoOfDay(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, LocalTime localTime) {
        Preconditions.checkArgument(localTime != null, "null localTime");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, localTime.toNanoOfDay());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public LocalTime fromParseableString(ParseContext ctx) {
        return LocalTime.parse(ctx.matchPrefix(LocalTimeType.PATTERN).group());
    }

    @Override
    public String toParseableString(LocalTime localTime) {
        return localTime.toString();
    }

    @Override
    public int compare(LocalTime localTime1, LocalTime localTime2) {
        return localTime1.compareTo(localTime2);
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

