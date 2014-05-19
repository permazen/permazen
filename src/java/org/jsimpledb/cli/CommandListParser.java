
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.util.ParseContext;

public class CommandListParser {

    public List<Action> parse(Session session, ParseContext ctx, boolean complete) {
        final ArrayList<Action> actions = new ArrayList<>();
        while (true) {
            ctx.skipWhitespace();
            if (ctx.isEOF() && !complete)
                break;
            actions.add(CommandParser.parse(session, ctx, complete));
            if (ctx.isEOF() && !complete)
                break;
            if (!ctx.tryLiteral(";"))
                throw new ParseException(ctx).addCompletion("; ");
        }
        return actions;
    }
}
