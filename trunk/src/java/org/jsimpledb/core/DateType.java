
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Date;

import org.dellroad.stuff.string.DateEncoder;
import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;

/**
 * Non-null {@link Date} type. Null values are not supported by this class.
 */
public class DateType extends FieldType<Date> {

    private static final byte[] DEFAULT_VALUE = new byte[] { (byte)LongEncoder.ZERO_ADJUST };

    DateType() {
        super(Date.class);
    }

// FieldType

    @Override
    public Date read(ByteReader reader) {
        return new Date(LongEncoder.read(reader));
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        for (int len = LongEncoder.decodeLength(reader.peek()); len > 0; len--)
            writer.writeByte(reader.readByte());
    }

    @Override
    public void write(ByteWriter writer, Date date) {
        if (date == null)
            throw new IllegalArgumentException("null date");
        LongEncoder.write(writer, date.getTime());
    }

    @Override
    public byte[] getDefaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Date fromString(ParseContext ctx) {
        ctx.expect('"');
        final Date date = DateEncoder.decode(ctx.matchPrefix(DateEncoder.PATTERN).group());
        ctx.expect('"');
        return date;
    }

    @Override
    public String toString(Date date) {
        return "\"" + DateEncoder.encode(date) + "\"";
    }

    @Override
    public int compare(Date date1, Date date2) {
        return date1.compareTo(date2);
    }

    @Override
    protected boolean hasPrefix0x00() {
        return false;
    }

    @Override
    protected boolean hasPrefix0xff() {
        return false;
    }
}

