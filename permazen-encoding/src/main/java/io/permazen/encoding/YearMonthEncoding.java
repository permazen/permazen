
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.time.YearMonth;
import java.util.OptionalInt;

/**
 * Non-null {@link YearMonth} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via two consecutive {@link LongEncoder}-encoded values, the {@linkplain YearMonth#getYear year}
 * followed by the {@linkplain YearMonth#getMonthValue month value}.
 */
public class YearMonthEncoding extends AbstractEncoding<YearMonth> {

    private static final long serialVersionUID = 2773124141026846109L;

    public YearMonthEncoding() {
        super(YearMonth.class);
    }

// Encoding

    @Override
    public YearMonth read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return YearMonth.of((int)LongEncoder.read(reader), (int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, YearMonth yearMonth) {
        Preconditions.checkArgument(yearMonth != null, "null yearMonth");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, yearMonth.getYear());
        LongEncoder.write(writer, yearMonth.getMonthValue());
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public YearMonth fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return YearMonth.parse(string);
    }

    @Override
    public String toString(YearMonth yearMonth) {
        Preconditions.checkArgument(yearMonth != null, "null yearMonth");
        return yearMonth.toString();
    }

    @Override
    public int compare(YearMonth yearMonth1, YearMonth yearMonth2) {
        return yearMonth1.compareTo(yearMonth2);
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
