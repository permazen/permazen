
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.ParseUtil;
import io.permazen.parse.Parser;
import io.permazen.util.ParseContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;

/**
 * Parses lambda expressions.
 */
public class LambdaExprParser implements Parser<LambdaNode> {

    public static final LambdaExprParser INSTANCE = new LambdaExprParser();

    @Override
    public LambdaNode parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Match one of: "() -> ...", "x -> ...", "(x) -> ..., "(x, y) -> ...", "(x, y, z) -> ...", etc.
        final String id = ParseUtil.IDENT_PATTERN;
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

        // Parse lambda expression with parameters in scope
        final Node body = Parser.applyIdentifierScope(ExprParser.INSTANCE, paramMap::get).parse(session, ctx, complete);

        // Done
        return new LambdaNode(new ArrayList<>(paramMap.values()), body);
    }
}
