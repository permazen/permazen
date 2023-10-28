
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.base.Converter;

import io.permazen.core.EncodingIds;

import java.io.Serializable;
import java.time.ZoneId;

/**
 * Non-null {@link ZoneId} type. Null values are not supported by this class.
 */
public class ZoneIdEncoding extends StringConvertedEncoding<ZoneId> {

    private static final long serialVersionUID = -4059733969700779261L;

    public ZoneIdEncoding() {
        super(EncodingIds.builtin("ZoneId"), ZoneId.class, new ZoneIdConverter());
    }

// ZoneIdConverter

    private static class ZoneIdConverter extends Converter<ZoneId, String> implements Serializable {

        private static final long serialVersionUID = -4059733969700779261L;

        @Override
        protected String doForward(ZoneId zoneId) {
            if (zoneId == null)
                return null;
            return zoneId.normalized().getId();
        }

        @Override
        protected ZoneId doBackward(String string) {
            if (string == null)
                return null;
            return ZoneId.of(string);
        }
    }
}
