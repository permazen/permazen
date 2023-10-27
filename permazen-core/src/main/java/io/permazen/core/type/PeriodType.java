
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.core.EncodingIds;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;
import io.permazen.util.ParseContext;

import java.time.Period;
import java.util.regex.Pattern;

/**
 * Non-null {@link Period} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via three consecutive {@link LongEncoder}-encoded values: the {@linkplain Period#getYears years},
 * the {@linkplain Period#getMonths months}, and the {@linkplain Period#getDays days}.
 */
public class PeriodType extends NonNullEncoding<Period> {

    private static final Pattern PATTERN = Pattern.compile("P(-?[0-9]+Y)?(-?[0-9]+M)?(-?[0-9]+?D)?|P-?[0-9]+W");

    private static final long serialVersionUID = -5481674489895732054L;

    public PeriodType() {
        super(EncodingIds.builtin("Period"), Period.class, Period.ZERO);
    }

// Encoding

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
