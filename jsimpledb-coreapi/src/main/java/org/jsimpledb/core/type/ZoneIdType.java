
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Converter;

import java.io.Serializable;
import java.time.ZoneId;

/**
 * Non-null {@link ZoneId} type. Null values are not supported by this class.
 */
public class ZoneIdType extends StringEncodedType<ZoneId> {

    private static final long serialVersionUID = -4059733969700779261L;

    public ZoneIdType() {
        super(ZoneId.class, 0, new ZoneIdConverter());
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
