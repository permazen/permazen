
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;

/**
 * Parses {@code instanceof} expressions.
 */
public class InstanceofParser implements Parser<Node> {

    public static final InstanceofParser INSTANCE = new InstanceofParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Parse left-hand side
        final Node node = ShiftExprParser.INSTANCE.parse(session, ctx, complete);

        // Try to parse 'instanceof some.class.name'
        final Matcher matcher = ctx.tryPattern("\\s*instanceof\\s+"
          + "(" + IdentNode.NAME_PATTERN + "(\\s*\\.\\s*" + IdentNode.NAME_PATTERN + ")*)\\s*");
        if (matcher == null)
            return node;
        final String className = matcher.group(1);

        // Resolve class name
        final Class<?> cl = session.resolveClass(className);
        if (cl == null)
            throw new EvalException("unknown class `" + className + "'");     // TODO: tab-completions

        // Return node that compares value's type to class
        return new Node() {
            @Override
            public Value evaluate(final ParseSession session) {
                return new ConstValue(cl.isInstance(node.evaluate(session).get(session)));
            }
        };
    }
}

