
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;

public class UnaryExprParser implements Parser<Node> {

    public static final UnaryExprParser INSTANCE = new UnaryExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Parse operator, if any
        final Matcher matcher = ctx.tryPattern("\\+{1,2}|-{1,2}|!|~");
        if (matcher == null)
            return BaseExprParser.INSTANCE.parse(session, ctx, complete);
        final String opsym = matcher.group();
        this.spaceParser.parse(ctx, complete);

        // Parse argument (this recursion will give the correct right-to-left association)
        final Node arg = this.parse(session, ctx, complete);

        // Proceed with parse
        switch (opsym) {
        case "!":
            return this.createUnaryOpNode(Op.LOGICAL_NOT, arg);
        case "~":
            return this.createUnaryOpNode(Op.INVERT, arg);
        case "+":
            return this.createUnaryOpNode(Op.UNARY_PLUS, arg);
        case "-":
            return this.createUnaryOpNode(Op.UNARY_MINUS, arg);
        case "++":
            return this.createPrecrementNode("increment", arg, true);
        case "--":
            return this.createPrecrementNode("decrement", arg, false);
        default:
            throw new RuntimeException("internal error: " + opsym);
        }
    }

    private Node createUnaryOpNode(final Op op, final Node arg) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return op.apply(session, arg.evaluate(session));
            }
        };
    }

    private Node createPrecrementNode(final String operation, final Node node, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return node.evaluate(session).xxcrement(session, "pre-" + operation, increment);
            }
        };
    }
}

