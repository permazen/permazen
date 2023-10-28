
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;
import io.permazen.util.ParseContext;

import java.time.LocalDate;
import java.util.regex.Pattern;

/**
 * Non-null {@link LocalDate} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via a single {@link LongEncoder}-encoded value representing the {@linkplain LocalDate#toEpochDay epoch day}.
 */
public class LocalDateEncoding extends BuiltinEncoding<LocalDate> {

    private static final Pattern PATTERN = Pattern.compile("-?[0-9]+-[0-9]+-[0-9]+");

    private static final long serialVersionUID = -1245720029314097665L;

    public LocalDateEncoding() {
        super(LocalDate.class);
    }

// Encoding

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
        return LocalDate.parse(ctx.matchPrefix(LocalDateEncoding.PATTERN).group());
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
