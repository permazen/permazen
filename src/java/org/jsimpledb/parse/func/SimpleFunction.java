
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import com.google.common.base.Preconditions;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

/**
 * Simplified {@link Function} implementation for when the parameters are all normal expressions.
 */
public abstract class SimpleFunction extends AbstractFunction {

    protected final int minArgs;
    protected final int maxArgs;

// Constructors

    /**
     * Constructor.
     *
     * @param name function name
     * @param minArgs minimum number of arguments (inclusive)
     * @param maxArgs maximum number of arguments (inclusive)
     */
    protected SimpleFunction(String name, int minArgs, int maxArgs) {
        super(name);
        Preconditions.checkArgument(minArgs >= 0 && minArgs <= maxArgs, "invalid minArgs/maxArgs");
        this.minArgs = minArgs;
        this.maxArgs = maxArgs;
    }

// Accessors

    /**
     * Get the minimum number of arguments allowed (inclusive).
     *
     * @return minimum required function arguments
     */
    public int getMinArgs() {
        return this.minArgs;
    }

    /**
     * Get the maximum number of arguments allowed (inclusive).
     *
     * @return maximum allowed function arguments
     */
    public int getMaxArgs() {
        return this.maxArgs;
    }

// Parsing

    @Override
    public final Node[] parseParams(ParseSession session, ParseContext ctx, boolean complete) {
        return this.parseExpressionParams(session, ctx, complete, 0, this.minArgs, this.maxArgs);
    }

    @Override
    public final Value apply(ParseSession session, Object info) {
        final Node[] params = (Node[])info;
        final Value[] values = new Value[params.length];
        for (int i = 0; i < params.length; i++)
            values[i] = params[i].evaluate(session);
        return this.apply(session, values);
    }

    /**
     * Apply this function to the given values.
     *
     * @param session parse session
     * @param params parsed parameters; will already be checked between {@link #getMinArgs} and {@link #getMaxArgs}
     * @return value returned by this function
     * @throws RuntimeException if there is an error
     */
    protected abstract Value apply(ParseSession session, Value[] params);
}

