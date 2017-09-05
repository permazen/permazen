
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.parse.SpaceParser;
import io.permazen.util.ParseContext;

import java.util.regex.Matcher;

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
        final Matcher castMatcher = ctx.tryPattern("(?s)\\(\\s*("
          + LiteralExprParser.CLASS_NAME_PATTERN + ")\\s*\\)\\s*(?=.)");
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
