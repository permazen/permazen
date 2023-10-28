
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

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
public class OffsetDateTimeEncoding extends Concat2Encoding<OffsetDateTime, Instant, ZoneOffset> {

    private static final long serialVersionUID = -1216769026293613698L;

    public OffsetDateTimeEncoding() {
        super(OffsetDateTime.class, new InstantEncoding(), new ZoneOffsetEncoding());
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

// Encoding

    @Override
    public OffsetDateTime fromParseableString(ParseContext ctx) {
        return OffsetDateTime.parse(ctx.matchPrefix(LocalDateTimeEncoding.PATTERN).group()
          + ctx.matchPrefix(ZoneOffsetEncoding.PATTERN).group());
    }

    @Override
    public String toParseableString(OffsetDateTime offsetDateTime) {
        return offsetDateTime.toString();
    }
}
