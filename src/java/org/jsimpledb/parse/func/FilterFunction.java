
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.ConstValue;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.Value;

@Function
public class FilterFunction extends ApplyExprFunction {

    public FilterFunction() {
        super("filter");
    }

    @Override
    public String getHelpSummary() {
        return "filters a collection";
    }

    @Override
    public String getUsage() {
        return "filters(items, variable, expression)";
    }

    @Override
    public String getHelpDetail() {
        return "Creates a filtered view of an Iterable, where items are included only if when the item is assigned to the"
          + " specified variable the specified expression evaluates to true. Maps are also supported, in which case the map's"
          + " entrySet() is filtered.";
    }

    @Override
    protected Value apply(ParseSession session, final ParamInfo params) {
        return new AbstractValue() {
            @Override
            public Object get(final ParseSession session) {

                // Build predicate
                final Predicate<Object> predicate = new Predicate<Object>() {
                    @Override
                    public boolean apply(Object item) {
                        return FilterFunction.this.evaluate(session, params.getVariable(),
                          new ConstValue(item), params.getExpr()).checkBoolean(session, "filter()");
                    }
                };

                // Evaluate items
                Object items = params.getItems().evaluate(session).checkNotNull(session, "filter()");
                if (items instanceof Map)
                    items = ((Map<?, ?>)items).entrySet();

                // Preserve NavigableSet if possible
                if (items instanceof NavigableSet)
                    return Sets.filter((NavigableSet<?>)items, predicate);

                // Preserve Set if possible
                if (items instanceof Set)
                    return Sets.filter((Set<?>)items, predicate);

                // Return Iterable view
                if (items instanceof Iterable)
                    return Iterables.filter((Iterable<?>)items, predicate);

                // Wrong type
                throw new EvalException("invalid filter() operation on non-Iterable object of type " + items.getClass().getName());
            }
        };
    }
}

