
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.WordParser;

public class CommandParser implements Parser<CliSession.Action> {

    /**
     * Parse a command and return the corresponding {@link CliSession.Action}.
     */
    @Override
    public CliSession.Action parse(ParseSession parseSession, ParseContext ctx, boolean complete) {
        final CliSession session = (CliSession)parseSession;
        final String commandName = new WordParser(session.getCommands().keySet(), "command").parse(session, ctx, complete);
        return session.getCommands().get(commandName).parse(session, ctx, complete);
    }
}

