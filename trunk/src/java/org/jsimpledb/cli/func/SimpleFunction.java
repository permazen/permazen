
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import java.util.ArrayList;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.cli.parse.expr.ExprParser;
import org.jsimpledb.cli.parse.expr.Node;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.util.ParseContext;

/**
 * Simplified {@link Function} implementation for when the parameters are all normal expressions.
 *
 * @see CliFunction
 */
public abstract class SimpleFunction extends Function {

    protected final int minArgs;
    protected final int maxArgs;

    private final SpaceParser spaceParser = new SpaceParser();

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
        if (minArgs < 0 || minArgs > maxArgs)
            throw new IllegalArgumentException("invalid minArgs = " + minArgs + ", maxArgs = " + maxArgs);
        this.minArgs = minArgs;
        this.maxArgs = maxArgs;
    }

// Accessors

    /**
     * Get the minimum number of arguments allowed (inclusive).
     */
    public int getMinArgs() {
        return this.minArgs;
    }

    /**
     * Get the maximum number of arguments allowed (inclusive).
     */
    public int getMaxArgs() {
        return this.maxArgs;
    }

// Parsing

    @Override
    public final Node[] parseParams(Session session, ParseContext ctx, boolean complete) {

        // Parse parameters
        final ArrayList<Node> params = new ArrayList<Node>(Math.min(this.maxArgs, this.minArgs * 2));
        while (true) {
            if (ctx.isEOF()) {
                final ParseException e = new ParseException(ctx, "truncated input");
                if (!params.isEmpty() && params.size() < this.minArgs)
                    e.addCompletion(", ");
                else if (params.size() >= this.minArgs)
                    e.addCompletion(") ");
                throw e;
            }
            if (ctx.tryLiteral(")"))
                break;
            if (!params.isEmpty()) {
                if (!ctx.tryLiteral(","))
                    throw new ParseException(ctx, "expected `,' between " + this.name + "() function parameters")
                      .addCompletion(", ");
                this.spaceParser.parse(ctx, complete);
            }
            params.add(ExprParser.INSTANCE.parse(session, ctx, complete));
            ctx.skipWhitespace();
        }

        // Check number
        if (params.size() < this.minArgs) {
            throw new ParseException(ctx, "at least " + this.minArgs + " argument(s) are required for function "
              + this.getName() + "()");
        } else if (params.size() > this.maxArgs) {
            throw new ParseException(ctx, "at most " + this.maxArgs + " argument(s) are allowed for function "
              + this.getName() + "()");
        }

        // Done
        return params.toArray(new Node[params.size()]);
    }

    @Override
    public final Value apply(Session session, Object info) {
        final Node[] params = (Node[])info;
        final Value[] values = new Value[params.length];
        for (int i = 0; i < params.length; i++)
            values[i] = params[i].evaluate(session);
        return this.apply(session, values);
    }

    /**
     * Apply this function to the given values.
     *
     * @param session CLI session
     * @param params parsed parameters; will already be checked between {@link #getMinArgs} and {@link #getMaxArgs}
     * @throws RuntimeException if there is an error
     */
    protected abstract Value apply(Session session, Value[] params);
}

