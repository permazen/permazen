
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli;

import com.google.common.base.Preconditions;

import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.WordParser;
import org.jsimpledb.util.ParseContext;

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

