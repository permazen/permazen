
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;
import io.permazen.util.ParseContext;

import java.util.Date;

import org.dellroad.stuff.string.DateEncoder;

/**
 * Non-null {@link Date} type. Null values are not supported by this class.
 */
public class DateType extends NonNullFieldType<Date> {

    private static final long serialVersionUID = 825120832596893074L;

    public DateType() {
        super(Date.class, 0);
    }

// FieldType

    @Override
    public Date read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return new Date(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, Date date) {
        Preconditions.checkArgument(date != null, "null date");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, date.getTime());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Date fromParseableString(ParseContext ctx) {
        return DateEncoder.decode(ctx.matchPrefix(DateEncoder.PATTERN).group());
    }

    @Override
    public String toParseableString(Date date) {
        return DateEncoder.encode(date);
    }

    @Override
    public int compare(Date date1, Date date2) {
        return date1.compareTo(date2);
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

