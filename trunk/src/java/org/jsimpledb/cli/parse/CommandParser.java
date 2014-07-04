
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.util.ParseContext;

public class CommandParser implements Parser<Action> {

    private final Session session;

    /**
     * Constructor.
     *
     * @param session associated {@link Session}
     * @throws IllegalArgumentException if {@code session} is null
     */
    public CommandParser(Session session) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        this.session = session;
    }

    /**
     * Parse a command and return the corresponding {@link Action}.
     */
    @Override
    public Action parse(Session session, ParseContext ctx, boolean complete) {
        final String commandName = new WordParser(this.session.getCommands().keySet(), "command").parse(session, ctx, complete);
        return this.session.getCommands().get(commandName).parse(session, ctx, complete);
    }
}

