
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

/**
 * Support superclass for {@link FieldType}'s that don't support null values.
 *
 * <p>
 * Such types may not be used standalone, but only within an outer type such as {@link NullSafeType}.
 */
abstract class NonNullFieldType<T> extends FieldType<T> {

// Constructors

    protected NonNullFieldType(String name, TypeToken<T> type, long signature, T defaultValue) {
        super(name, type, signature, defaultValue);
    }

    protected NonNullFieldType(Class<T> type, long signature, T defaultValue) {
       super(type, signature, defaultValue);
    }

// Constructors for when there is no default value

    protected NonNullFieldType(String name, TypeToken<T> type, long signature) {
        this(name, type, signature, null);
    }

    protected NonNullFieldType(Class<T> type, long signature) {
       this(type, signature, null);
    }

    @Override
    public T validate(Object obj) {
        Preconditions.checkArgument(obj != null, "invalid null value");
        return super.validate(obj);
    }
}

