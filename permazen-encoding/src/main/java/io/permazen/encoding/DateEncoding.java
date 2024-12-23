
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.util.Date;
import java.util.OptionalInt;

import org.dellroad.stuff.string.DateEncoder;

/**
 * Non-null {@link Date} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 */
public class DateEncoding extends AbstractEncoding<Date> {

    private static final long serialVersionUID = 825120832596893074L;

    public DateEncoding() {
        super(Date.class);
    }

// Encoding

    @Override
    public Date read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return new Date(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, Date date) {
        Preconditions.checkArgument(date != null, "null date");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, date.getTime());
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Date fromString(String string) {
        return DateEncoder.decode(string);
    }

    @Override
    public String toString(Date date) {
        Preconditions.checkArgument(date != null, "null date");
        return DateEncoder.encode(date);
    }

    @Override
    public int compare(Date date1, Date date2) {
        return date1.compareTo(date2);
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
