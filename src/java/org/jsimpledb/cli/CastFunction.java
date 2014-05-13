
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

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
        return this.type.cast(obj);
    }
}

