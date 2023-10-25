
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import io.permazen.core.EncodingIds;

/**
 * Primitive wrapper type.
 *
 * @param <T> Java primitive wrapper type
 */
public class PrimitiveWrapperType<T> extends NullSafeType<T> {

    private static final long serialVersionUID = 3988749140485792884L;

    public PrimitiveWrapperType(PrimitiveType<T> primitiveType) {
        super(EncodingIds.builtin(primitiveType.primitive.getLongName()), primitiveType);
    }
}

