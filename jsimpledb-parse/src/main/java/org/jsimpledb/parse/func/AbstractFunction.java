
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.EnumSet;

import org.jsimpledb.SessionMode;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for {@link Function}s.
 *
 * @see Function
 */
public abstract class AbstractFunction implements Function {

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
        Preconditions.checkArgument(name != null, "null name");
        this.name = name;
    }

// Accessors

    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The implementation in {@link AbstractFunction} just delegates to {@link #getHelpSummary getHelpSummary()}.
     *
     * @return detailed description of this function
     */
    @Override
    public String getHelpDetail() {
        return this.getHelpSummary();
    }

    /**
     * Get the {@link SessionMode}(s) supported by this instance.
     *
     * <p>
     * The implementation in {@link AbstractFunction} returns an {@link EnumSet} containing
     * {@link SessionMode#CORE_API} and {@link SessionMode#JSIMPLEDB}.
     *
     * @return supported {@link SessionMode}s
     */
    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.<SessionMode>of(SessionMode.CORE_API, SessionMode.JSIMPLEDB);
    }

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
    protected Node[] parseExpressionParams(ParseSession session, ParseContext ctx, boolean complete,
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
