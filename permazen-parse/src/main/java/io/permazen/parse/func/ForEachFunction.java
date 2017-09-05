
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import java.util.EnumSet;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.ConstValue;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Value;

public class ForEachFunction extends ApplyExprFunction {

    public ForEachFunction() {
        super("foreach");
    }

    @Override
    public String getHelpSummary() {
        return "Evaluates an expression for each item in a collection";
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
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
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
            this.evaluate(session, params.getVariable(), new ConstValue(item), params.getExpr());

        // Done
        return Value.NO_VALUE;
    }
}
