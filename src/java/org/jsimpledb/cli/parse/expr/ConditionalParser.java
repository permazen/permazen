
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.util.ParseContext;

public class ConditionalParser implements Parser<Node> {

    public static final ConditionalParser INSTANCE = new ConditionalParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {

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
            public Value evaluate(Session session) {
                return test.evaluate(session).checkBoolean(session, "conditional") ?
                  ifTrue.evaluate(session) : ifFalse.evaluate(session);
            }
        };
    }
}

