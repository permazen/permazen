
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.parse.WordParser;
import io.permazen.util.ParseContext;

public class CommandParser implements Parser<CliSession.Action> {

    /**
     * Parse a command and return the corresponding {@link CliSession.Action}.
     *
     * @throws IllegalArgumentException if {@code session} is not a {@link CliSession}
     */
    @Override
    public CliSession.Action parse(ParseSession parseSession, ParseContext ctx, boolean complete) {
        Preconditions.checkArgument(parseSession instanceof CliSession, "session is not a CliSession");
        final CliSession session = (CliSession)parseSession;
        final String commandName = new WordParser(session.getCommands().keySet(), "command").parse(session, ctx, complete);
        return session.getCommands().get(commandName).parse(session, ctx, complete);
    }
}

