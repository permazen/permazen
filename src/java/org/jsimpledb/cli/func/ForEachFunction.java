
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import java.util.Map;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;

@CliFunction
public class ForEachFunction extends ApplyExprFunction {

    public ForEachFunction() {
        super("foreach");
    }

    @Override
    public String getHelpSummary() {
        return "iterates over a collection";
    }

    @Override
    public String getUsage() {
        return "foreach(items, variable, expression)";
    }

    @Override
    public String getHelpDetail() {
        return "Iterates over a collection, for each item assigning the item to the specified variable and evaluating"
          + " the specified expression.";
    }

    @Override
    protected Value apply(Session session, ParamInfo params) {

        // Evaluate items
        Object items = params.getItems().evaluate(session).checkNotNull(session, "foreach()");

        // Iterate over items and evaluate expression
        if (items instanceof Map)
            items = ((Map<?, ?>)items).entrySet();
        if (!(items instanceof Iterable))
            throw new EvalException("invalid foreach() operation over object of type " + items.getClass().getName());
        for (Object item : ((Iterable<?>)items))
            this.evaluate(session, params.getVariable(), item, params.getExpr());

        // Done
        return new Value(null);
    }
}

