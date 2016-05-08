
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

/**
 * Primitive wrapper type.
 */
class PrimitiveWrapperType<T> extends NullSafeType<T> {

    PrimitiveWrapperType(PrimitiveType<T> primitiveType) {
        super(primitiveType.primitive.getWrapperType().getName(), primitiveType);
    }
}

