
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * A {@link Function} that casts objects to some type.
 */
public class CastFunction<T> implements Function<Object, T> {

    protected final Class<T> type;

    /**
     * Constructor.
     *
     * @param type desired type
     * @throws IllegalArgumentException if {@code type} is null
     */
     public CastFunction(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        this.type = type;
    }

    @Override
    public T apply(Object obj) {
        if (obj == null)
            return null;
        try {
            return this.type.cast(obj);
        } catch (ClassCastException e) {
            throw this.handleFailure(obj, e);
        }
    }

    /**
     * Generate an exception to throw when a cast failure occurs.
     *
     * <p>
     * The implementation in {@link CastFunction} just throws {@code e}.
     * </p>
     *
     * @param obj object on which cast failed
     * @param e resulting exception
     * @return exception to throw
     */
    protected RuntimeException handleFailure(Object obj, ClassCastException e) {
        return e;
    }
}

