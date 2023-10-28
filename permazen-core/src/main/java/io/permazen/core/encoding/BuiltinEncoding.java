
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.reflect.TypeToken;

import io.permazen.core.AbstractEncoding;
import io.permazen.core.Encoding;
import io.permazen.core.EncodingIds;

/**
 * Support superclass for {@link Encoding}'s that don't support null values.
 *
 * <p>
 * Except for primitive types, such types may not be used standalone, but only within an outer type such as {@link NullSafeEncoding}.
 */
abstract class BuiltinEncoding<T> extends AbstractEncoding<T> {

    private static final long serialVersionUID = 5533087685258954052L;

    BuiltinEncoding(Class<T> type, T defaultValue) {
        super(EncodingIds.builtin(type.getSimpleName()), TypeToken.of(type), defaultValue);
    }

    BuiltinEncoding(Class<T> type) {
        this(type, null);
    }
}
