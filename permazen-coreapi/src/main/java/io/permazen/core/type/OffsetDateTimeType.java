
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import io.permazen.core.EncodingIds;
import io.permazen.util.ParseContext;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Non-null {@link OffsetDateTime} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via the concatenation of the encodings of {@link Instant} and {@link ZoneOffset}.
 */
public class OffsetDateTimeType extends Concat2Type<OffsetDateTime, Instant, ZoneOffset> {

    private static final long serialVersionUID = -1216769026293613698L;

    public OffsetDateTimeType() {
        super(EncodingIds.builtin("OffsetDateTime"), OffsetDateTime.class, new InstantType(), new ZoneOffsetType());
    }

    @Override
    protected OffsetDateTime join(Instant value1, ZoneOffset value2) {
        return OffsetDateTime.ofInstant(value1, value2);
    }

    @Override
    protected Instant split1(OffsetDateTime value) {
        return value.toInstant();
    }

    @Override
    protected ZoneOffset split2(OffsetDateTime value) {
        return value.getOffset();
    }

// FieldType

    @Override
    public OffsetDateTime fromParseableString(ParseContext ctx) {
        return OffsetDateTime.parse(ctx.matchPrefix(LocalDateTimeType.PATTERN).group()
          + ctx.matchPrefix(ZoneOffsetType.PATTERN).group());
    }

    @Override
    public String toParseableString(OffsetDateTime offsetDateTime) {
        return offsetDateTime.toString();
    }
}
