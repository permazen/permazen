
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;

import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link ZonedDateTime} type. Null values are not supported by this class.
 */
public class ZonedDateTimeType extends Concat2Type<ZonedDateTime, OffsetDateTime, ZoneId> {

    private static final long serialVersionUID = 1L;

    public ZonedDateTimeType() {
        super(ZonedDateTime.class, 0, new OffsetDateTimeType(), new ZoneIdType());
    }

    @Override
    protected ZonedDateTime join(OffsetDateTime value1, ZoneId value2) {
        return ZonedDateTime.ofInstant(value1.toInstant(), value2);
    }

    @Override
    protected OffsetDateTime split1(ZonedDateTime value) {
        return value.toOffsetDateTime();
    }

    @Override
    protected ZoneId split2(ZonedDateTime value) {
        return value.getZone();
    }

// FieldType

    @Override
    public ZonedDateTime fromParseableString(ParseContext ctx) {
        final OffsetDateTime offsetDateTime = this.type1.fromParseableString(ctx);
        final Matcher matcher = ctx.tryPattern("\\[(.+)\\]");
        final ZoneId zoneId = matcher != null ? this.type2.fromString(matcher.group(1)) : offsetDateTime.getOffset();
        return ZonedDateTime.ofInstant(offsetDateTime.toInstant(), zoneId);
    }

    @Override
    public String toParseableString(ZonedDateTime zonedDateTime) {
        return zonedDateTime.toString();
    }
}

