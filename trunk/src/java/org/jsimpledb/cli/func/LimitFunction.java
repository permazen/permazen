
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.util.Iterator;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;

@CliFunction
public class LimitFunction extends SimpleFunction {

    public LimitFunction() {
        super("limit", 2, 2);
    }

    @Override
    public String getUsage() {
        return "limit(items, max)";
    }

    @Override
    public String getHelpSummary() {
        return "discards items past a maximum count";
    }

    @Override
    protected Value apply(Session session, Value[] params) {

        // Get limit
        final Value value = params[0];
        final int limit = params[1].checkNumeric(session, "limit()").intValue();
        if (limit < 0)
            throw new EvalException("invalid limit() value " + limit);

        // Apply limit
        return new Value(null) {
            @Override
            public Object get(Session session) {
                final Object obj = value.checkNotNull(session, "limit()");
                if (obj instanceof Iterable)
                    return Iterables.limit((Iterable<?>)obj, limit);
                if (obj instanceof Iterator)
                    return Iterators.limit((Iterator<?>)obj, limit);
                throw new EvalException("limit() cannot be applied to object of type " + obj.getClass().getName());
            }
        };
    }
}

