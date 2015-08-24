
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * Parses logical OR expressions of the form {@code x || y}.
 */
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
            public Value evaluate(ParseSession session) {
                for (Node node : new Node[] { lhNode, rhNode }) {
                    if (node.evaluate(session).checkBoolean(session, "logical `or'"))
                        return new ConstValue(true);
                }
                return new ConstValue(false);
            }
        };
    }
}

