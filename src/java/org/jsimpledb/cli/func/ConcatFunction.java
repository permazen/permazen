
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import com.google.common.collect.Iterables;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;

@CliFunction
public class ConcatFunction extends SimpleFunction {

    public ConcatFunction() {
        super("concat", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "concatenates the Iterables within an Iterable";
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
    protected Value apply(Session session, Value[] params) {
        final Value value = params[0];
        return new Value(null) {
            @Override
            public Object get(final Session session) {

                // Evaluate items
                final Object items = value.checkNotNull(session, "concat()");
                if (!(items instanceof Iterable)) {
                    throw new EvalException("invalid concat() operation on non-Iterable object of type "
                      + items.getClass().getName());
                }

                // Return concatenation of Iterables
                return Iterables.concat(Iterables.transform((Iterable<?>)items,
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

