
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.util.ParseContext;

/**
 * Expression parser.
 */
public class ExprParser implements Parser<Node> {

    public static final ExprParser INSTANCE = new ExprParser();

    private final AssignmentExprParser assignmentExprParser = new AssignmentExprParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {
        return this.assignmentExprParser.parse(session, ctx, complete);
    }
}

