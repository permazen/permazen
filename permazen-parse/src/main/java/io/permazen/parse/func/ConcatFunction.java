
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import com.google.common.collect.Iterables;

import java.util.EnumSet;

import io.permazen.SessionMode;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.AbstractValue;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Value;

public class ConcatFunction extends SimpleFunction {

    public ConcatFunction() {
        super("concat", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "Concatenates the Iterables within an Iterable";
    }

    @Override
    public String getUsage() {
        return "concat(items)";
    }

    @Override
    public String getHelpDetail() {
        return "The concat() function takes an argument which must be an Iterable of Iterables (i.e., an instance of"
          + " Iterable<? extends Iterable<?>>), and returns a new Iterable<?> which is the concatenation of the inner Iterables.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected Value apply(ParseSession session, Value[] params) {
        final Value value = params[0];
        return new AbstractValue() {
            @Override
            public Object get(final ParseSession session) {
                return Iterables.concat(Iterables.transform((Iterable<?>)value.checkType(session, "concat()", Iterable.class),
                  new com.google.common.base.Function<Object, Iterable<?>>() {
                    @Override
                    public Iterable<?> apply(Object item) {
                        if (item == null)
                            throw new EvalException("concat() operation encountered null Iterable");
                        if (!(item instanceof Iterable)) {
                            throw new EvalException("concat() operation encountered non-Iterable value of type "
                              + item.getClass().getName());
                        }
                        return (Iterable<?>)item;
                    }
                }));
            }
        };
    }
}
