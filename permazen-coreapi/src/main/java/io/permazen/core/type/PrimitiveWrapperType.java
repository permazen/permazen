
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

/**
 * Primitive wrapper type.
 */
public class PrimitiveWrapperType<T> extends NullSafeType<T> {

    private static final long serialVersionUID = 3988749140485792884L;

    public PrimitiveWrapperType(PrimitiveType<T> primitiveType) {
        super(primitiveType.primitive.getWrapperType().getName(), primitiveType);
    }
}

