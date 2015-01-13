
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

/**
 * Support superclass for {@link FieldType}'s that don't support null values.
 */
abstract class NonNullFieldType<T> extends FieldType<T> {

    protected NonNullFieldType(String name, TypeToken<T> type, long signature) {
        super(name, type, signature);
    }

    protected NonNullFieldType(Class<T> type, long signature) {
       super(type, signature);
    }

    /**
     * Get the default value for this field type encoded as a {@code byte[]} array.
     *
     * <p>
     * The implementation in {@link NonNullFieldType} always throws {@link UnsupportedOperationException}.
     * </p>
     */
    @Override
    public byte[] getDefaultValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T validate(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("invalid null value");
        return super.validate(obj);
    }
}

