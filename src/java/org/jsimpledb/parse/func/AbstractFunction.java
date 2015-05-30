
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import java.util.ArrayList;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Superclass of all {@link ParseSession} functions.
 *
 * @see Function
 */
public abstract class AbstractFunction {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final SpaceParser spaceParser = new SpaceParser();
    protected final String name;

// Constructors

    /**
     * Constructor.
     *
     * @param name function name
     */
    protected AbstractFunction(String name) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.name = name;
    }

// Accessors

    /**
     * Get the name of this function.
     *
     * @return function name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get function usage string. For example: {@code pow(base, exponent)}.
     *
     * @return function usage string
     */
    public abstract String getUsage();

    /**
     * Get summarized help (typically a single line).
     *
     * @return one line summarized description of this function
     */
    public abstract String getHelpSummary();

    /**
     * Get expanded help (typically multiple lines). May refer to the {@linkplain #getUsage usage string}.
     *
     * <p>
     * The implementation in {@link AbstractFunction} delegates to {@link #getHelpSummary getHelpSummary()}.
     * </p>
     *
     * @return detailed description of this function
     */
    public String getHelpDetail() {
        return this.getHelpSummary();
    }

// Parsing

    /**
     * Parse function parameters.
     *
     * <p>
     * The {@code ctx} will be pointing at the first parameter (if any) or closing parenthesis. This method should parse
     * (but not evaluate) function parameters up through the closing parenthesis.
     * </p>
     *
     * @param session parse session
     * @param ctx parse context
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @return parsed parameters object to be passed to {@link #apply apply()}
     * @throws ParseException if parse fails, or if {@code complete} is true and there are valid completions
     */
    public abstract Object parseParams(ParseSession session, ParseContext ctx, boolean complete);

    /**
     * Evaluate this function. There will be a transaction open.
     *
     * @param session parse session
     * @param params parsed parameters returned by {@link #parseParams parseParams()}
     * @return value returned by this function
     * @throws RuntimeException if there is an error
     */
    public abstract Value apply(ParseSession session, Object params);

    /**
     * Parse some number of Java expression function arguments. We assume we have parsed the opening parenthesis,
     * zero or more previous arguments followed by commas, and optional whitespace. This will parse through
     * the closing parenthesis.
     *
     * @param session parse session
     * @param ctx parse context
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @param skippedArgs the number of arguments already parsed
     * @param minArgs minimum number of arguments
     * @param maxArgs maximum number of arguments
     * @return parsed expressions
     * @throws ParseException if parse fails, or if {@code complete} is true and there are valid completions
     */
    public Node[] parseExpressionParams(ParseSession session, ParseContext ctx, boolean complete,
      int skippedArgs, int minArgs, int maxArgs) {

        // Parse parameters
        final ArrayList<Node> params = new ArrayList<Node>(Math.min(maxArgs, minArgs * 2));
        while (true) {
            if (ctx.isEOF()) {
                final ParseException e = new ParseException(ctx, "truncated input");
                if (!params.isEmpty() && params.size() < minArgs)
                    e.addCompletion(", ");
                else if (params.size() >= minArgs)
                    e.addCompletion(")");
                throw e;
            }
            if (ctx.tryLiteral(")")) {
                if (params.size() < minArgs) {
                    throw new ParseException(ctx, "at least " + (skippedArgs + minArgs) + " argument(s) are required for function "
                      + this.getName() + "()");
                }
                break;
            }
            if (params.size() >= maxArgs) {
                throw new ParseException(ctx, "at most " + (skippedArgs + maxArgs) + " argument(s) are allowed for function "
                  + this.getName() + "()");
            }
            if (!params.isEmpty()) {
                if (!ctx.tryLiteral(",")) {
                    throw new ParseException(ctx, "expected `,' between " + this.getName() + "() function parameters")
                      .addCompletion(", ");
                }
                this.spaceParser.parse(ctx, complete);
            }
            params.add(ExprParser.INSTANCE.parse(session, ctx, complete));
            ctx.skipWhitespace();
        }

        // Done
        return params.toArray(new Node[params.size()]);
    }
}

