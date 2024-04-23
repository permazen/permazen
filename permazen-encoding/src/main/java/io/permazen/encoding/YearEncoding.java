
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;

import java.time.Year;

/**
 * Non-null {@link Year} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is the {@link LongEncoder}-encoded {@linkplain Year#getValue year value}.
 */
public class YearEncoding extends AbstractEncoding<Year> {

    private static final long serialVersionUID = 6800527893478605289L;

    public YearEncoding(EncodingId encodingId) {
        super(encodingId, Year.class, Year.of(0));
    }

// Encoding

    @Override
    public YearEncoding withEncodingId(EncodingId encodingId) {
        return new YearEncoding(encodingId);
    }

    @Override
    public Year read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return Year.of((int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, Year year) {
        Preconditions.checkArgument(year != null, "null year");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, year.getValue());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Year fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return Year.parse(string);
    }

    @Override
    public String toString(Year year) {
        Preconditions.checkArgument(year != null, "null year");
        int value = year.getValue();
        String sign = "";
        if (value < 0) {
            value = -value;
            sign = "-";
        }
        if (value > 9999)
            sign = "+";
        return String.format("%s%04d", sign, value);
    }

    @Override
    public int compare(Year year1, Year year2) {
        return year1.compareTo(year2);
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
