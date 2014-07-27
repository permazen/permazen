
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import java.util.Map;

import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.Value;

@Function
public class ForEachFunction extends ApplyExprFunction {

    public ForEachFunction() {
        super("foreach");
    }

    @Override
    public String getHelpSummary() {
        return "evaluates an expression for each item in a collection";
    }

    @Override
    public String getUsage() {
        return "foreach(items, variable, expression)";
    }

    @Override
    public String getHelpDetail() {
        return "Iterates over an Iterable, for each item assigning the item to the specified variable and evaluating"
          + " the specified expression. Maps are also supported, in which case the map's entrySet() is iterated.";
    }

    @Override
    protected Value apply(ParseSession session, ParamInfo params) {

        // Evaluate items
        Object items = params.getItems().evaluate(session).checkNotNull(session, "foreach()");

        // Iterate over items and evaluate expression
        if (items instanceof Map)
            items = ((Map<?, ?>)items).entrySet();
        if (!(items instanceof Iterable))
            throw new EvalException("invalid foreach() operation over non-Iterable object of type " + items.getClass().getName());
        for (Object item : ((Iterable<?>)items))
            this.evaluate(session, params.getVariable(), new Value(item), params.getExpr());

        // Done
        return Value.NO_VALUE;
    }
}

