
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.time.Duration;
import java.util.OptionalInt;

/**
 * Non-null {@link Duration} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via two consecutive {@link LongEncoder}-encoded values, {@linkplain Duration#getSeconds seconds}
 * followed by {@linkplain Duration#getNano nanoseconds}.
 */
public class DurationEncoding extends AbstractEncoding<Duration> {

    private static final long serialVersionUID = 969067179729229705L;

    public DurationEncoding() {
        super(Duration.class);
    }

// Encoding

    @Override
    public Duration read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return Duration.ofSeconds(LongEncoder.read(reader), (int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, Duration duration) {
        Preconditions.checkArgument(duration != null, "null duration");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, duration.getSeconds());
        LongEncoder.write(writer, duration.getNano());
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Duration fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return Duration.parse(string);
    }

    @Override
    public String toString(Duration duration) {
        Preconditions.checkArgument(duration != null, "null duration");
        return duration.toString();
    }

    @Override
    public int compare(Duration duration1, Duration duration2) {
        return duration1.compareTo(duration2);
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
