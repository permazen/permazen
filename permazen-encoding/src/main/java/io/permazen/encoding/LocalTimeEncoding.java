
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;

import java.time.LocalTime;

/**
 * Non-null {@link LocalTime} type.
 *
 * <p>
 * Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via a single {@link LongEncoder}-encoded value representing
 * {@linkplain LocalTime#toNanoOfDay nanoseconds in the day}.
 */
public class LocalTimeEncoding extends AbstractEncoding<LocalTime> {

    private static final long serialVersionUID = -6138317689607411426L;

    public LocalTimeEncoding(EncodingId encodingId) {
        super(encodingId, LocalTime.class, LocalTime.ofSecondOfDay(0));
    }

// Encoding

    @Override
    public LocalTimeEncoding withEncodingId(EncodingId encodingId) {
        return new LocalTimeEncoding(encodingId);
    }

    @Override
    public LocalTime read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return LocalTime.ofNanoOfDay(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, LocalTime localTime) {
        Preconditions.checkArgument(localTime != null, "null localTime");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, localTime.toNanoOfDay());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public LocalTime fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return LocalTime.parse(string);
    }

    @Override
    public String toString(LocalTime localTime) {
        Preconditions.checkArgument(localTime != null, "null localTime");
        return localTime.toString();
    }

    @Override
    public int compare(LocalTime localTime1, LocalTime localTime2) {
        return localTime1.compareTo(localTime2);
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
