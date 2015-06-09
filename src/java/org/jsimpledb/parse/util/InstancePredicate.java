
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

/**
 * A {@link Predicate} that tests whether given objects are instances of a certain type.
 */
public class InstancePredicate implements Predicate<Object> {

    private final Class<?> type;

    public InstancePredicate(Class<?> type) {
        Preconditions.checkArgument(type != null, "null type");
        this.type = type;
    }

    @Override
    public boolean apply(Object obj) {
        return obj != null && this.type.isInstance(obj);
    }
}

