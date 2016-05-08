
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.util.ParseContext;

/**
 * Parses {@code instanceof} expressions.
 */
public class InstanceofParser implements Parser<Node> {

    public static final InstanceofParser INSTANCE = new InstanceofParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Parse left-hand side
        final Node target = ShiftExprParser.INSTANCE.parse(session, ctx, complete);

        // Try to parse 'instanceof some.class.name'
        final Matcher matcher = ctx.tryPattern("\\s*instanceof\\s+(" + LiteralExprParser.CLASS_NAME_PATTERN + ")\\s*");
        if (matcher == null)
            return target;
        final String className = matcher.group(1).replaceAll("\\s+", "");
        final ClassNode classNode = ClassNode.parse(ctx, className, false);

        // Return node that compares value's type to class
        return new Node() {
            @Override
            public Value evaluate(final ParseSession session) {
                return new ConstValue(classNode.resolveClass(session).isInstance(target.evaluate(session).get(session)));
            }

            @Override
            public Class<?> getType(ParseSession session) {
                return Boolean.class;
            }
        };
    }
}

