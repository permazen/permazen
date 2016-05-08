
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.util.ParseContext;

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

