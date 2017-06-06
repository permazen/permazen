
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import java.time.MonthDay;
import java.util.regex.Pattern;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.ParseContext;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Non-null {@link MonthDay} type. Null values are not supported by this class.
 */
public class MonthDayType extends NonNullFieldType<MonthDay> {

    private static final Pattern PATTERN = Pattern.compile("--[0-9]{2}-[0-9]{2}");

    private static final long serialVersionUID = -8813919603844250786L;

    public MonthDayType() {
        super(MonthDay.class, 0);
    }

// FieldType

    @Override
    public MonthDay read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        final int value = UnsignedIntEncoder.read(reader);
        return MonthDay.of((value >> 5) + 1, (value & 0x1f) + 1);
    }

    @Override
    public void write(ByteWriter writer, MonthDay monthDay) {
        Preconditions.checkArgument(monthDay != null, "null monthDay");
        Preconditions.checkArgument(writer != null);
        final int month = monthDay.getMonthValue();
        final int day = monthDay.getDayOfMonth();
        UnsignedIntEncoder.write(writer, ((month - 1) << 5) | (day - 1));
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(UnsignedIntEncoder.decodeLength(reader.peek()));
    }

    @Override
    public MonthDay fromParseableString(ParseContext ctx) {
        return MonthDay.parse(ctx.matchPrefix(MonthDayType.PATTERN).group());
    }

    @Override
    public String toParseableString(MonthDay monthDay) {
        return monthDay.toString();
    }

    @Override
    public int compare(MonthDay monthDay1, MonthDay monthDay2) {
        return monthDay1.compareTo(monthDay2);
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

