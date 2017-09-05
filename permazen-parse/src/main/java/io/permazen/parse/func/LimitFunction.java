
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.util.EnumSet;
import java.util.Iterator;

import io.permazen.SessionMode;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.AbstractValue;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Value;

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
        return "Discards items past a maximum count";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected Value apply(ParseSession session, Value[] params) {

        // Get limit
        final Value value = params[0];
        final int limit = params[1].checkNumeric(session, "limit()").intValue();
        if (limit < 0)
            throw new EvalException("invalid limit() value " + limit);

        // Apply limit
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
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

