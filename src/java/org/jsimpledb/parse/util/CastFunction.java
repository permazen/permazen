
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.util;

import com.google.common.base.Function;

import org.jsimpledb.parse.expr.EvalException;

/**
 * Casts to a type.
 */
public class CastFunction<T> implements Function<Object, T> {

    protected final Class<T> type;

    public CastFunction(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
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
     * The implementation in {@link CastFunction} returns an {@link EvalException}
     * </p>
     *
     * @param obj object on which cast failed
     * @param e resulting exception
     * @return exception to throw
     */
    protected RuntimeException handleFailure(Object obj, ClassCastException e) {
        return new EvalException("can't cast object of type " + obj.getClass().getName() + " to " + this.type.getName());
    }
}

