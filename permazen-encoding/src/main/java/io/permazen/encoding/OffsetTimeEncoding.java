
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

/**
 * Non-null {@link OffsetTime} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
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

    public OffsetTimeEncoding(EncodingId encodingId) {
        super(encodingId, OffsetTime.class, null, new LongEncoding(null), new ZoneOffsetEncoding(null));
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
    public OffsetTime fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return OffsetTime.parse(string);
    }

    @Override
    public String toString(OffsetTime offsetTime) {
        Preconditions.checkArgument(offsetTime != null, "null offsetTime");
        return offsetTime.toString();
    }

    @Override
    public boolean sortsNaturally() {
        return true;
    }
}
