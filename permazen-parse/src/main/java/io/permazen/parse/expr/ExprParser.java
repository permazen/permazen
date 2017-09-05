
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.util.ParseContext;

/**
 * Java expression parser.
 */
public class ExprParser implements Parser<Node> {

    public static final ExprParser INSTANCE = new ExprParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Try lambda
        final int start = ctx.getIndex();
        try {
            return LambdaExprParser.INSTANCE.parse(session, ctx, complete);
        } catch (ParseException e) {
            ctx.setIndex(start);
        }

        // Parse assignment
        return AssignmentExprParser.INSTANCE.parse(session, ctx, complete);
    }
}

