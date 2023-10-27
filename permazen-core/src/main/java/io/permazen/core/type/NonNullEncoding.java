
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.AbstractEncoding;
import io.permazen.core.Encoding;
import io.permazen.core.EncodingId;

/**
 * Support superclass for {@link Encoding}'s that don't support null values.
 *
 * <p>
 * Except for primitive types, such types may not be used standalone, but only within an outer type such as {@link NullSafeType}.
 */
public abstract class NonNullEncoding<T> extends AbstractEncoding<T> {

    private static final long serialVersionUID = 5533087685258954052L;

    protected NonNullEncoding(EncodingId encodingId, Class<T> type, T defaultValue) {
        super(encodingId, TypeToken.of(type), defaultValue);
    }

    protected NonNullEncoding(EncodingId encodingId, TypeToken<T> type, T defaultValue) {
        super(encodingId, type, defaultValue);
    }

    // Constructor for when there is no default value
    protected NonNullEncoding(EncodingId encodingId, Class<T> type) {
        this(encodingId, type, null);
    }

    // Constructor for when there is no default value
    protected NonNullEncoding(EncodingId encodingId, TypeToken<T> type) {
        this(encodingId, type, null);
    }

    @Override
    public T validate(Object obj) {
        Preconditions.checkArgument(obj != null, "invalid null value");
        return super.validate(obj);
    }
}
