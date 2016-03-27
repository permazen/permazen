
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AtomExprParser;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.expr.VarNode;

public abstract class ApplyExprFunction extends AbstractFunction {

    protected ApplyExprFunction(String name) {
        super(name);
    }

    @Override
    public ParamInfo parseParams(ParseSession session, ParseContext ctx, boolean complete) {

        // Get items
        final int mark = ctx.getIndex();
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "three parameters required for " + this.name + "()");
        final Node items = new ExprParser().parse(session, ctx, complete);

        // Get variable
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,'").addCompletion(", ");
        this.spaceParser.parse(ctx, complete);
        final Node param1 = AtomExprParser.INSTANCE.parse(session, ctx, complete);
        if (!(param1 instanceof VarNode)) {
            ctx.setIndex(mark);
            throw new ParseException(ctx, "expected variable as second parameter to " + this.name + "()");
        }
        final String variable = ((VarNode)param1).getName();

        // Get expression
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,'").addCompletion(", ");
        this.spaceParser.parse(ctx, complete);
        final Node expr = new ExprParser().parse(session, ctx, complete);

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return new ParamInfo(variable, items, expr);
    }

    @Override
    public final Value apply(ParseSession session, Object params) {
        return this.apply(session, (ParamInfo)params);
    }

    /**
     * Apply this function.
     *
     * @param session parse session
     * @param params function parameters
     * @return the value of this function
     */
    protected abstract Value apply(ParseSession session, ParamInfo params);

    /**
     * Set the variable to the given value and then evaluate the expression.
     * Any previous value of the variable is saved prior and restored after.
     *
     * @param session parse session
     * @param variable the name of the variable to use
     * @param value the value to set to the variable
     * @param expr the expression to evaluate
     * @return the value of the expression
     */
    protected Value evaluate(ParseSession session, String variable, Value value, Node expr) {

        // Save previous variable value, if any
        final boolean hasPreviousValue = session.getVars().containsKey(variable);
        final Value previousValue = hasPreviousValue ? session.getVars().get(variable) : null;

        // Assign variable
        session.getVars().put(variable, value);
        final Value result;
        try {

            // Evaluate expression
            result = expr.evaluate(session);

        } finally {

            // Restore previous variable value
            if (hasPreviousValue)
                session.getVars().put(variable, previousValue);
        }

        // Done
        return result;
    }

// ParamInfo

    static class ParamInfo {

        private final String variable;
        private final Node items;
        private final Node expr;

        ParamInfo(String variable, Node items, Node expr) {
            this.variable = variable;
            this.items = items;
            this.expr = expr;
        }

        public String getVariable() {
            return this.variable;
        }

        public Node getItems() {
            return this.items;
        }

        public Node getExpr() {
            return this.expr;
        }
    }
}

