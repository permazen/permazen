
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Non-null {@link OffsetDateTime} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via the concatenation of the encodings of {@link Instant} and {@link ZoneOffset}.
 */
public class OffsetDateTimeEncoding extends Concat2Encoding<OffsetDateTime, Instant, ZoneOffset> {

    private static final long serialVersionUID = -1216769026293613698L;

    public OffsetDateTimeEncoding() {
        super(OffsetDateTime.class, new InstantEncoding(), new ZoneOffsetEncoding(),
          OffsetDateTime::toInstant, OffsetDateTime::getOffset, OffsetDateTime::ofInstant);
    }

// Encoding

    @Override
    public OffsetDateTime fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return OffsetDateTime.parse(string);
    }

    @Override
    public String toString(OffsetDateTime offsetDateTime) {
        Preconditions.checkArgument(offsetDateTime != null, "null offsetDateTime");
        return offsetDateTime.toString();
    }

    @Override
    public boolean sortsNaturally() {
        return true;
    }
}
