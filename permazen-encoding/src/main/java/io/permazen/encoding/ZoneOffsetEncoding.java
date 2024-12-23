
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.time.ZoneOffset;
import java.util.OptionalInt;

/**
 * Non-null {@link ZoneOffset} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via the {@link LongEncoder}-encoded negative of the
 * {@linkplain ZoneOffset#getTotalSeconds total seconds value}. The value is negated because higher offsets
 * {@linkplain ZoneOffset#compareTo sort} before lower ones.
 */
public class ZoneOffsetEncoding extends AbstractEncoding<ZoneOffset> {

    private static final long serialVersionUID = 4606196393878370203L;

    public ZoneOffsetEncoding() {
        super(ZoneOffset.class);
    }

// Encoding

    @Override
    public ZoneOffset read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return ZoneOffset.ofTotalSeconds(-(int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, ZoneOffset zoneOffset) {
        Preconditions.checkArgument(zoneOffset != null, "null zoneOffset");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, -zoneOffset.getTotalSeconds());       // negated because e.g. +10:00 is before +5:00
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public ZoneOffset fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return ZoneOffset.of(string);
    }

    @Override
    public String toString(ZoneOffset zoneOffset) {
        Preconditions.checkArgument(zoneOffset != null, "null zoneOffset");
        return zoneOffset.getId();
    }

    @Override
    public int compare(ZoneOffset zoneOffset1, ZoneOffset zoneOffset2) {
        return zoneOffset1.compareTo(zoneOffset2);
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
