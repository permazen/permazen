
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import java.time.Period;
import java.util.regex.Pattern;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link Period} type. Null values are not supported by this class.
 */
public class PeriodType extends NonNullFieldType<Period> {

    private static final Pattern PATTERN = Pattern.compile("P(-?[0-9]+Y)?(-?[0-9]+M)?(-?[0-9]+?D)?|P-?[0-9]+W");

    private static final long serialVersionUID = 1L;

    public PeriodType() {
        super(Period.class, 0, Period.ZERO);
    }

// FieldType

    @Override
    public Period read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return Period.of((int)LongEncoder.read(reader), (int)LongEncoder.read(reader), (int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, Period period) {
        Preconditions.checkArgument(period != null, "null period");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, period.getYears());
        LongEncoder.write(writer, period.getMonths());
        LongEncoder.write(writer, period.getDays());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Period fromParseableString(ParseContext ctx) {
        return Period.parse(ctx.matchPrefix(PeriodType.PATTERN).group());
    }

    @Override
    public String toParseableString(Period period) {
        return period.toString();
    }

    @Override
    public int compare(Period period1, Period period2) {
        int diff = Integer.compare(period1.getYears(), period2.getYears());
        if (diff != 0)
            return diff;
        diff = Integer.compare(period1.getMonths(), period2.getMonths());
        if (diff != 0)
            return diff;
        diff = Integer.compare(period1.getDays(), period2.getDays());
        if (diff != 0)
            return diff;
        return 0;
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

