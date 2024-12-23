
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.time.MonthDay;
import java.util.OptionalInt;

/**
 * Non-null {@link MonthDay} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via an {@link UnsignedIntEncoder}-encoded value {@code 32} times the
 * {@linkplain MonthDay#getMonthValue month value}{@code - 1}, plus the {@linkplain MonthDay#getDayOfMonth day of the month}.
 */
public class MonthDayEncoding extends AbstractEncoding<MonthDay> {

    private static final long serialVersionUID = -8813919603844250786L;

    public MonthDayEncoding() {
        super(MonthDay.class);
    }

// Encoding

    @Override
    public MonthDay read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final int value = UnsignedIntEncoder.read(reader);
        return MonthDay.of((value >> 5) + 1, (value & 0x1f) + 1);
    }

    @Override
    public void write(ByteData.Writer writer, MonthDay monthDay) {
        Preconditions.checkArgument(monthDay != null, "null monthDay");
        Preconditions.checkArgument(writer != null);
        final int month = monthDay.getMonthValue();
        final int day = monthDay.getDayOfMonth();
        UnsignedIntEncoder.write(writer, ((month - 1) << 5) | (day - 1));
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(UnsignedIntEncoder.decodeLength(reader.peek()));
    }

    @Override
    public MonthDay fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return MonthDay.parse(string);
    }

    @Override
    public String toString(MonthDay monthDay) {
        Preconditions.checkArgument(monthDay != null, "null monthDay");
        return monthDay.toString();
    }

    @Override
    public int compare(MonthDay monthDay1, MonthDay monthDay2) {
        return monthDay1.compareTo(monthDay2);
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
