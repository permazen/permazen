
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link OffsetTime} type. Null values are not supported by this class.
 */
public class OffsetTimeType extends Concat2Type<OffsetTime, Long, ZoneOffset> {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private static final long serialVersionUID = -42507926581583354L;

    public OffsetTimeType() {
        super(OffsetTime.class, 0, new LongType(), new ZoneOffsetType());
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

// FieldType

    @Override
    public OffsetTime fromParseableString(ParseContext ctx) {
        return OffsetTime.parse(ctx.matchPrefix(LocalTimeType.PATTERN).group() + ctx.matchPrefix(ZoneOffsetType.PATTERN).group());
    }

    @Override
    public String toParseableString(OffsetTime offsetTime) {
        return offsetTime.toString();
    }
}
