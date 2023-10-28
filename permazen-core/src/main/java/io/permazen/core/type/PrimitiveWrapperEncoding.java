
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
public class PrimitiveWrapperEncoding<T> extends NullSafeEncoding<T> {

    private static final long serialVersionUID = 3988749140485792884L;

    public PrimitiveWrapperEncoding(PrimitiveEncoding<T> primitiveType) {
        super(EncodingIds.builtin(primitiveType.primitive.getLongName()), primitiveType);
    }
}
