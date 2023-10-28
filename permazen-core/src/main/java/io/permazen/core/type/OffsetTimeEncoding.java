
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import io.permazen.util.ParseContext;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

/**
 * Non-null {@link OffsetTime} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via the concatenation of the encodings of {@link LongEncoding} and {@link ZoneOffset}, where the first value
 * is the {@linkplain LocalTime#toNanoOfDay nanoseconds in the day} normalized to {@link ZoneOffset#UTC UTC}.
 * This keeps the binary sort order consistent with {@link OffsetTime#compareTo}.
 */
public class OffsetTimeEncoding extends Concat2Encoding<OffsetTime, Long, ZoneOffset> {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private static final long serialVersionUID = -42507926581583354L;

    public OffsetTimeEncoding() {
        super(OffsetTime.class, new LongEncoding(), new ZoneOffsetEncoding());
    }

    @Override
    protected OffsetTime join(Long value1, ZoneOffset value2) {
        return OffsetTime.of(LocalTime.ofNanoOfDay(value1 + value2.getTotalSeconds() * NANOS_PER_SECOND), value2);
    }

    @Override
    protected Long split1(OffsetTime value) {
        return value.toLocalTime().toNanoOfDay() - value.getOffset().getTotalSeconds() * NANOS_PER_SECOND;
    }

    @Override
    protected ZoneOffset split2(OffsetTime value) {
        return value.getOffset();
    }

// Encoding

    @Override
    public OffsetTime fromParseableString(ParseContext ctx) {
        return OffsetTime.parse(ctx.matchPrefix(LocalTimeEncoding.PATTERN).group()
          + ctx.matchPrefix(ZoneOffsetEncoding.PATTERN).group());
    }

    @Override
    public String toParseableString(OffsetTime offsetTime) {
        return offsetTime.toString();
    }
}
