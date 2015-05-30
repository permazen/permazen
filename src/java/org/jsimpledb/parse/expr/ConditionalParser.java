
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;

/**
 * Parses conditional expressions of the form {@code x ? y : z}.
 */
public class ConditionalParser implements Parser<Node> {

    public static final ConditionalParser INSTANCE = new ConditionalParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Parse test
        final Node test = LogicalOrParser.INSTANCE.parse(session, ctx, complete);

        // Parse '?'
        this.spaceParser.parse(ctx, complete);
        if (!ctx.tryLiteral("?"))
            return test;

        // Parse true result
        this.spaceParser.parse(ctx, complete);
        final Node ifTrue = LogicalOrParser.INSTANCE.parse(session, ctx, complete);

        // Parse ':'
        this.spaceParser.parse(ctx, complete);
        if (!ctx.tryLiteral(":"))
            throw new ParseException(ctx).addCompletion(": ");

        // Parse false result
        this.spaceParser.parse(ctx, complete);
        final Node ifFalse = LogicalOrParser.INSTANCE.parse(session, ctx, complete);

        // Return node
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return test.evaluate(session).checkBoolean(session, "conditional") ?
                  ifTrue.evaluate(session) : ifFalse.evaluate(session);
            }
        };
    }
}

