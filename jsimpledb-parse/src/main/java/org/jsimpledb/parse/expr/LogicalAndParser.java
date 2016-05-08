
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * Parses logical AND expressions of the form {@code x && y}.
 */
public class LogicalAndParser extends BinaryExprParser {

    public static final LogicalAndParser INSTANCE = new LogicalAndParser();

    public LogicalAndParser() {
        super(BitwiseOrParser.INSTANCE, Op.LOGICAL_AND);
    }

    // Overridden to provide short-circuit logic
    @Override
    protected Node createNode(final Op op, final Node lhNode, final Node rhNode) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                for (Node node : new Node[] { lhNode, rhNode }) {
                    if (!node.evaluate(session).checkBoolean(session, "logical `and'"))
                        return new ConstValue(false);
                }
                return new ConstValue(true);
            }

            @Override
            public Class<?> getType(ParseSession session) {
                return Boolean.class;
            }
        };
    }
}

