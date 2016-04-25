
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;

/**
 * Parses type cast expressions.
 */
public class CastExprParser implements Parser<Node> {

    public static final CastExprParser INSTANCE = new CastExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Try cast
        final int start = ctx.getIndex();
        final Matcher castMatcher = ctx.tryPattern("\\(\\s*(" + LiteralExprParser.CLASS_NAME_PATTERN + ")\\s*\\)\\s*(?=.)");
        while (castMatcher != null) {
            final String className = castMatcher.group(1).replaceAll("\\s", "");

            // Parse target of cast
            final Node target;
            try {
                target = this.parse(session, ctx, complete);             // associates right-to-left
                ctx.skipWhitespace();
                return new CastNode(new ClassNode(className, true), target);
            } catch (ParseException e) {
                ctx.setIndex(start);
            }
        }

        // Try lambda
        try {
            return LambdaExprParser.INSTANCE.parse(session, ctx, complete);
        } catch (ParseException e) {
            ctx.setIndex(start);
        }

        // Parse unary
        return UnaryExprParser.INSTANCE.parse(session, ctx, complete);
    }
}
