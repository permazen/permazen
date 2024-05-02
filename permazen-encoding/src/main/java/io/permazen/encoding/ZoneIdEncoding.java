
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;

import java.time.ZoneId;

/**
 * {@link ZoneId} type.
 *
 * <p>
 * Null values are supported by this class.
 */
public class ZoneIdEncoding extends StringConvertedEncoding<ZoneId> {

    private static final long serialVersionUID = -4059733969700779261L;

    public ZoneIdEncoding(EncodingId encodingId) {
        super(encodingId, ZoneId.class, Converter.from(zoneId -> zoneId.normalized().getId(), ZoneId::of));
    }
}
