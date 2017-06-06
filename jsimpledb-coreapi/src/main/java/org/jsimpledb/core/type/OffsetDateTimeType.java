
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link OffsetDateTime} type. Null values are not supported by this class.
 */
public class OffsetDateTimeType extends Concat2Type<OffsetDateTime, Instant, ZoneOffset> {

    private static final long serialVersionUID = -1216769026293613698L;

    public OffsetDateTimeType() {
        super(OffsetDateTime.class, 0, new InstantType(), new ZoneOffsetType());
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
