
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.util;

import com.google.common.base.Predicate;

/**
 * A {@link Predicate} that tests whether given objects are instances of a certain type.
 */
public class InstancePredicate implements Predicate<Object> {

    private final Class<?> type;

    public InstancePredicate(Class<?> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.type = type;
    }

    @Override
    public boolean apply(Object obj) {
        return obj != null && this.type.isInstance(obj);
    }
}

