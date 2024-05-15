
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Non-null {@link ZonedDateTime} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via the concatenation of the encodings of {@link OffsetDateTimeEncoding} and {@link ZoneIdEncoding}.
 */
public class ZonedDateTimeEncoding extends Concat2Encoding<ZonedDateTime, OffsetDateTime, ZoneId> {

    private static final long serialVersionUID = 2484375470437659420L;

    public ZonedDateTimeEncoding() {
        super(ZonedDateTime.class, new OffsetDateTimeEncoding(), new ZoneIdEncoding(null),
          ZonedDateTime::toOffsetDateTime, ZonedDateTime::getZone,
          (value1, value2) ->  ZonedDateTime.ofInstant(value1.toInstant(), value2));
    }

// Encoding

    @Override
    public ZonedDateTime fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        final OffsetDateTime offsetDateTime;
        final ZoneId zoneId;
        final int bracket = string.indexOf('[');
        if (bracket != -1 && string.charAt(string.length() - 1) == ']') {
            offsetDateTime = this.getTuple2Encoding().getEncoding1().fromString(string.substring(0, bracket));
            zoneId = this.getTuple2Encoding().getEncoding2().fromString(string.substring(bracket + 1, string.length() - 1));
        } else {
            offsetDateTime = this.getTuple2Encoding().getEncoding1().fromString(string);
            zoneId = offsetDateTime.getOffset();
        }
        return ZonedDateTime.ofInstant(offsetDateTime.toInstant(), zoneId);
    }

    @Override
    public String toString(ZonedDateTime zonedDateTime) {
        Preconditions.checkArgument(zonedDateTime != null, "null zonedDateTime");
        return zonedDateTime.toString();
    }

    @Override
    public boolean sortsNaturally() {
        return true;
    }
}
