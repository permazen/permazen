
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import java.time.ZoneOffset;
import java.util.regex.Pattern;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link ZoneOffset} type. Null values are not supported by this class.
 */
public class ZoneOffsetType extends NonNullFieldType<ZoneOffset> {

    static final Pattern PATTERN = Pattern.compile("(Z|[-+][0-9]{2}:[0-9]{2}(:[0-9]{2})?)");

    private static final long serialVersionUID = 1L;

    public ZoneOffsetType() {
        super(ZoneOffset.class, 0);
    }

// FieldType

    @Override
    public ZoneOffset read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return ZoneOffset.ofTotalSeconds(-(int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, ZoneOffset zoneOffset) {
        Preconditions.checkArgument(zoneOffset != null, "null zoneOffset");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, -zoneOffset.getTotalSeconds());       // negated because e.g. +10:00 is before +5:00
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public ZoneOffset fromParseableString(ParseContext ctx) {
        return ZoneOffset.of(ctx.matchPrefix(ZoneOffsetType.PATTERN).group());
    }

    @Override
    public String toParseableString(ZoneOffset zoneOffset) {
        return zoneOffset.getId();
    }

    @Override
    public int compare(ZoneOffset zoneOffset1, ZoneOffset zoneOffset2) {
        return zoneOffset1.compareTo(zoneOffset2);
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

