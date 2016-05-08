
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import com.google.common.collect.Iterables;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.ConstValue;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.Value;

@Function(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class TransformFunction extends ApplyExprFunction {

    public TransformFunction() {
        super("transform");
    }

    @Override
    public String getHelpSummary() {
        return "Transforms a collection";
    }

    @Override
    public String getUsage() {
        return "transform(items, variable, expression)";
    }

    @Override
    public String getHelpDetail() {
        return "Creates a view of an Iterable where each item is transformed by assigning the item to the specified variable"
          + " and evaluating the specified expression. Maps are also supported, in which case the map's entrySet() is transformed.";
    }

    @Override
    protected Value apply(ParseSession session, final ParamInfo params) {
        return new AbstractValue() {
            @Override
            public Object get(final ParseSession session) {

                // Evaluate items
                Object items = params.getItems().evaluate(session).checkNotNull(session, "transform()");
                if (items instanceof Map)
                    items = ((Map<?, ?>)items).entrySet();
                if (!(items instanceof Iterable)) {
                    throw new EvalException("invalid transform() operation on non-Iterable object of type "
                      + items.getClass().getName());
                }

                // Return tranformed view
                return Iterables.transform((Iterable<?>)items, new com.google.common.base.Function<Object, Object>() {
                    @Override
                    public Object apply(Object item) {
                        return TransformFunction.this.evaluate(session,
                          params.getVariable(), new ConstValue(item), params.getExpr()).get(session);
                    }
                });
            }
        };
    }
}

