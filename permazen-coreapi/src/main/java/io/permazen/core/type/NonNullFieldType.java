
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.FieldType;

/**
 * Support superclass for {@link FieldType}'s that don't support null values.
 *
 * <p>
 * Except for primitive types, such types may not be used standalone, but only within an outer type such as {@link NullSafeType}.
 */
public abstract class NonNullFieldType<T> extends FieldType<T> {

    private static final long serialVersionUID = 5533087685258954052L;

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

