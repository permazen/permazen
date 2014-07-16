
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import java.util.regex.Matcher;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.util.ParseContext;

public class UnaryExprParser implements Parser<Node> {

    public static final UnaryExprParser INSTANCE = new UnaryExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {

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
            public Value evaluate(Session session) {
                return op.apply(session, arg.evaluate(session));
            }
        };
    }

    private Node createPrecrementNode(final String operation, final Node arg, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(Session session) {
                return arg.evaluate(session).xxcrement(session, "pre-" + operation, increment);
            }
        };
    }
}

