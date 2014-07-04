
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.util.ParseContext;

public class CommandListParser implements Parser<List<Action>> {

    private final Parser<Action> commandParser;

    /**
     * Constructor.
     *
     * @param commandParser single command parser
     */
    public CommandListParser(Parser<Action> commandParser) {
        if (commandParser == null)
            throw new IllegalArgumentException("null commandParser");
        this.commandParser = commandParser;
    }

    /**
     * Parse one or more commands and return the {@link Action}s corresponding to the parsed commands.
     */
    @Override
    public List<Action> parse(Session session, ParseContext ctx, boolean complete) {
        final ArrayList<Action> actions = new ArrayList<>();
        while (true) {
            ctx.skipWhitespace();
            if (ctx.isEOF() && !complete)
                break;
            actions.add(this.commandParser.parse(session, ctx, complete));
            if (ctx.isEOF() && !complete)
                break;
            if (!ctx.tryLiteral(";"))
                throw new ParseException(ctx).addCompletion("; ");
        }
        return actions;
    }
}

