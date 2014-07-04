
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import org.jsimpledb.cli.Session;

public class LogicalOrParser extends BinaryExprParser {

    public static final LogicalOrParser INSTANCE = new LogicalOrParser();

    public LogicalOrParser() {
        super(LogicalAndParser.INSTANCE, Op.LOGICAL_OR);
    }

    // Overridden to provide short-circuit logic
    @Override
    protected Node createNode(final Op op, final Node lhNode, final Node rhNode) {
        return new Node() {
            @Override
            public Value evaluate(Session session) {
                for (Node node : new Node[] { lhNode, rhNode }) {
                    if (node.evaluate(session).checkBoolean(session, "logical `or'"))
                        return new Value(true);
                }
                return new Value(false);
            }
        };
    }
}

