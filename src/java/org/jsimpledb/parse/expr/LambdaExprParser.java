
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.Parser;

/**
 * Parses lambda expressions.
 */
public class LambdaExprParser implements Parser<LambdaNode> {

    public static final LambdaExprParser INSTANCE = new LambdaExprParser();

    @Override
    public LambdaNode parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Match one of: "() -> ...", "x -> ...", "(x) -> ..., "(x, y) -> ", "(x, y, z) -> ", etc.
        final String id = IdentNode.NAME_PATTERN.toString();
        final Matcher matcher = ctx.tryPattern("((" + id + ")|\\(\\s*(" + id + "\\s*(,\\s*" + id + "\\s*)*)?\\))\\s*->\\s*");
        if (matcher == null)
            throw new ParseException(ctx);

        // Get identifier list and create nodes for the parsed params
        final LinkedHashMap<String, LambdaNode.Param> paramMap = new LinkedHashMap<>(4);
        if (matcher.group(2) != null) {
            final String name = matcher.group(2);
            paramMap.put(name, new LambdaNode.Param(name));
        } else if (matcher.group(3) != null) {
            for (String name : matcher.group(3).trim().split("\\s*,\\s*")) {
                if (paramMap.containsKey(name))
                    throw new ParseException(ctx, "duplicate lambda parameter `" + name + "'");
                paramMap.put(name, new LambdaNode.Param(name));
            }
        }

        // Put parameters in scope while parsing lambda body
        final Parser<? extends Node> previousParser = session.getIdentifierParser();
        session.setIdentifierParser(new Parser<Node>() {
            @Override
            public Node parse(ParseSession session, ParseContext ctx, boolean complete) {
                final String name = ctx.tryPattern(IdentNode.NAME_PATTERN).group();
                final LambdaNode.Param paramNode = paramMap.get(name);
                if (paramNode == null) {
                    throw new ParseException(ctx, "unknown lambda parameter `" + name + "'")
                      .addCompletions(ParseUtil.complete(paramMap.keySet(), name));
                }
                return paramNode;
            }
        });
        final Node body;
        try {
            body = ExprParser.INSTANCE.parse(session, ctx, complete);
        } finally {
            session.setIdentifierParser(previousParser);
        }

        // Done
        return new LambdaNode(new ArrayList<>(paramMap.values()), body);
    }
}
