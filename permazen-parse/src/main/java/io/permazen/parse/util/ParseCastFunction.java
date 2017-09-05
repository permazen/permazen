
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.util;

import io.permazen.parse.expr.EvalException;
import io.permazen.util.CastFunction;

/**
 * Parsing specific subclass of {@link CastFunction} that throws {@link EvalException}s instead of {@link ClassCastException}s.
 */
public class ParseCastFunction<T> extends CastFunction<T> {

    public ParseCastFunction(Class<T> type) {
        super(type);
    }

    @Override
    protected RuntimeException handleFailure(Object obj, ClassCastException e) {
        return new EvalException("can't cast object of type " + obj.getClass().getName() + " to " + this.type.getName());
    }
}

