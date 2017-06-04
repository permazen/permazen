
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import java.time.Instant;
import java.util.regex.Pattern;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link Instant} type. Null values are not supported by this class.
 */
public class InstantType extends NonNullFieldType<Instant> {

    private static final Pattern PATTERN = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?Z");

    private static final long serialVersionUID = 1L;

    public InstantType() {
        super(Instant.class, 0);
    }

// FieldType

    @Override
    public Instant read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return Instant.ofEpochSecond(LongEncoder.read(reader), (int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, Instant instant) {
        Preconditions.checkArgument(instant != null, "null instant");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, instant.getEpochSecond());
        LongEncoder.write(writer, instant.getNano());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Instant fromParseableString(ParseContext ctx) {
        return Instant.parse(ctx.matchPrefix(InstantType.PATTERN).group());
    }

    @Override
    public String toParseableString(Instant instant) {
        return instant.toString();
    }

    @Override
    public int compare(Instant instant1, Instant instant2) {
        return instant1.compareTo(instant2);
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

