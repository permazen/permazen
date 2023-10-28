
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;
import io.permazen.util.ParseContext;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Non-null {@link Duration} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via two consecutive {@link LongEncoder}-encoded values, {@linkplain Duration#getSeconds seconds}
 * followed by {@linkplain Duration#getNano nanoseconds}.
 */
public class DurationType extends BuiltinEncoding<Duration> {

    private static final Pattern PATTERN = Pattern.compile("PT(-?[0-9]+H)?(-?[0-9]+M)?(-?[0-9]+(\\.[0-9]+)?S)?");

    private static final long serialVersionUID = 969067179729229705L;

    public DurationType() {
        super(Duration.class, Duration.ZERO);
    }

// Encoding

    @Override
    public Duration read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return Duration.ofSeconds(LongEncoder.read(reader), (int)LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, Duration duration) {
        Preconditions.checkArgument(duration != null, "null duration");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, duration.getSeconds());
        LongEncoder.write(writer, duration.getNano());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public Duration fromParseableString(ParseContext ctx) {
        return Duration.parse(ctx.matchPrefix(DurationType.PATTERN).group());
    }

    @Override
    public String toParseableString(Duration duration) {
        return duration.toString();
    }

    @Override
    public int compare(Duration duration1, Duration duration2) {
        return duration1.compareTo(duration2);
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
