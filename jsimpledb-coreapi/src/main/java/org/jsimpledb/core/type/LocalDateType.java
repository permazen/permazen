
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import java.time.LocalDate;
import java.util.regex.Pattern;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link LocalDate} type. Null values are not supported by this class.
 */
public class LocalDateType extends NonNullFieldType<LocalDate> {

    private static final Pattern PATTERN = Pattern.compile("-?[0-9]+-[0-9]+-[0-9]+");

    private static final long serialVersionUID = 1L;

    public LocalDateType() {
        super(LocalDate.class, 0);
    }

// FieldType

    @Override
    public LocalDate read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return LocalDate.ofEpochDay(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, LocalDate localDate) {
        Preconditions.checkArgument(localDate != null, "null localDate");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, localDate.toEpochDay());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public LocalDate fromParseableString(ParseContext ctx) {
        return LocalDate.parse(ctx.matchPrefix(LocalDateType.PATTERN).group());
    }

    @Override
    public String toParseableString(LocalDate localDate) {
        return localDate.toString();
    }

    @Override
    public int compare(LocalDate localDate1, LocalDate localDate2) {
        return localDate1.compareTo(localDate2);
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

