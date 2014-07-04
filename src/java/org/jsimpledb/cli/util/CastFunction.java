
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.util;

import com.google.common.base.Function;

/**
 * Casts to a type.
 */
public class CastFunction<T> implements Function<Object, T> {

    private final Class<T> type;

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
            throw new IllegalArgumentException("can't cast object of type " + obj.getClass().getName()
              + " to " + this.type.getName());
        }
    }
}

