
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.time.Period;
import java.util.OptionalInt;

/**
 * Non-null {@link Period} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via three consecutive {@link LongEncoder}-encoded values: the {@linkplain Period#getYears years},
 * the {@linkplain Period#getMonths months}, and the {@linkplain Period#getDays days}.
 */
public class PeriodEncoding extends AbstractEncoding<Period> {

    private static final long serialVersionUID = -5481674489895732054L;

    public PeriodEncoding() {
        super(Period.class);
    }

// Encoding

    @Override
    public Period read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return Period.of((int)LongEncoder.read(reader), (int)LongEncoder.read(reader), (int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, Period period) {
        Preconditions.checkArgument(period != null, "null period");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, period.getYears());
        LongEncoder.write(writer, period.getMonths());
        LongEncoder.write(writer, period.getDays());
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Period fromString(String string) {
        return Period.parse(string);
    }

    @Override
    public String toString(Period period) {
        Preconditions.checkArgument(period != null, "null period");
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
    public boolean supportsNull() {
        return false;
    }

    @Override
    public boolean sortsNaturally() {
        return false;
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
