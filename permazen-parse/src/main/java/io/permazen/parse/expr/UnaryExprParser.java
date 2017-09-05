
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import java.util.regex.Matcher;

import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.parse.SpaceParser;
import io.permazen.util.ParseContext;

/**
 * Parses unary expressions using one of {@code !}, {@code ~}, {@code +}, {@code -}, {@code ++}, {@code --}.
 */
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
            return this.createUnaryOpNode(Op.LOGICAL_NOT, arg, Boolean.class);
        case "~":
            return this.createUnaryOpNode(Op.INVERT, arg, null);
        case "+":
            return this.createUnaryOpNode(Op.UNARY_PLUS, arg, Number.class);
        case "-":
            return this.createUnaryOpNode(Op.UNARY_MINUS, arg, Number.class);
        case "++":
            return this.createPrecrementNode("increment", arg, null, true);
        case "--":
            return this.createPrecrementNode("decrement", arg, null, false);
        default:
            throw new RuntimeException("internal error: " + opsym);
        }
    }

    private Node createUnaryOpNode(final Op op, final Node arg, final Class<?> type) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return op.apply(session, arg.evaluate(session));
            }

            @Override
            public Class<?> getType(ParseSession session) {
                return type != null ? type : arg.getType(session);
            }
        };
    }

    private Node createPrecrementNode(final String operation, final Node node, final Class<?> type, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return node.evaluate(session).xxcrement(session, "pre-" + operation, increment);
            }

            @Override
            public Class<?> getType(ParseSession session) {
                return type != null ? type : node.getType(session);
            }
        };
    }
}

