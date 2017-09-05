
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.util.ParseContext;

import java.util.ArrayList;
import java.util.List;

public class CommandListParser implements Parser<List<CliSession.Action>> {

    private final Parser<CliSession.Action> commandParser;

    /**
     * Constructor.
     *
     * @param commandParser single command parser
     */
    public CommandListParser(Parser<CliSession.Action> commandParser) {
        Preconditions.checkArgument(commandParser != null, "null commandParser");
        this.commandParser = commandParser;
    }

    /**
     * Parse one or more commands and return the {@link CliSession.Action}s corresponding to the parsed commands.
     */
    @Override
    public List<CliSession.Action> parse(ParseSession session, ParseContext ctx, boolean complete) {
        final ArrayList<CliSession.Action> actions = new ArrayList<>();
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

