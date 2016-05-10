
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.util.ParseContext;

/**
 * Support superclass for tail-recursive binary expression parsers for expressions of the form {@code ARG1 OP ARG2}.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Tail_recursive_parser">Tail Recursive Parser</a>
 */
public abstract class BinaryExprParser implements Parser<Node> {

    private final SpaceParser spaceParser = new SpaceParser();
    private final Parser<? extends Node> lowerLevel;
    private final boolean leftAssociative;
    private final List<Op> ops;

    /**
     * Primary constructor.
     *
     * @param lowerLevel next lower parse level
     * @param leftAssociative true for left-to-right association, false for right-to-left
     * @param ops operations, with operations that have other operations as prefixes listed first
     */
    protected BinaryExprParser(Parser<? extends Node> lowerLevel, boolean leftAssociative, Op... ops) {
        Preconditions.checkArgument(lowerLevel != null, "null lowerLevel");
        Preconditions.checkArgument(ops != null && ops.length > 0, "null/empty ops");
        this.lowerLevel = lowerLevel;
        this.leftAssociative = leftAssociative;
        this.ops = Arrays.asList(ops);
    }

    /**
     * Convenience constructor for left-associative operators.
     *
     * @param lowerLevel next lower parse level
     * @param ops operations, with operations that have other operations as prefixes listed first
     */
    protected BinaryExprParser(Parser<? extends Node> lowerLevel, Op... ops) {
        this(lowerLevel, true, ops);
    }

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Gather sub-nodes and intervening operators
        final ArrayList<Node> nodeList = new ArrayList<>(2);
        final ArrayList<Op> opList = new ArrayList<>(1);
        nodeList.add(this.lowerLevel.parse(session, ctx, complete));
        while (true) {
            this.spaceParser.parse(ctx, complete);
            final int mark = ctx.getIndex();
            Op op = null;
            for (Op candidate : this.ops) {
                if (ctx.tryLiteral(candidate.getSymbol())) {
                    op = candidate;
                    break;
                }
            }
            if (op == null)
                break;
            this.spaceParser.parse(ctx, complete);
            final Node rhs;
            try {
                rhs = this.lowerLevel.parse(session, ctx, complete);
            } catch (ParseException e) {
                if (complete && !e.getCompletions().isEmpty())
                    throw e;
                ctx.setIndex(mark);                             // backtrack
                break;
            }
            nodeList.add(rhs);
            opList.add(op);
        }
        if (nodeList.size() == 1)
            return nodeList.get(0);

        // Build AST
        Node node;
        if (this.leftAssociative) {
            node = this.createNode(opList.get(0), nodeList.get(0), nodeList.get(1));
            for (int i = 1; i < nodeList.size() - 1; i++)
                node = this.createNode(opList.get(i), node, nodeList.get(i + 1));
        } else {
            final int max = nodeList.size();
            node = this.createNode(opList.get(max - 2), nodeList.get(max - 2), nodeList.get(max - 1));
            for (int i = max - 3; i >= 0; i--)
                node = this.createNode(opList.get(i), nodeList.get(i), node);
        }

        // Done
        return node;
    }

    protected Node createNode(final Op op, final Node lhs, final Node rhs) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return op.apply(session, lhs.evaluate(session), rhs.evaluate(session));
            }

            @Override
            public Class<?> getType(ParseSession session) {
                final Class<?> ltype = lhs.getType(session);
                final Class<?> rtype = rhs.getType(session);
                return ltype == rtype ? ltype : Object.class;           // TODO: be more precise
            }
        };
    }
}

