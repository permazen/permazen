
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;

/**
 * Java expression parser.
 */
public class ExprParser implements Parser<Node> {

    public static final ExprParser INSTANCE = new ExprParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {
        return AssignmentExprParser.INSTANCE.parse(session, ctx, complete);
    }
}

