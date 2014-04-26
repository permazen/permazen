
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;

import org.dellroad.stuff.string.ParseContext;

/**
 * Help command for a "leaf" (non-aggregate) command.
 */
public class HelpCommandCommand extends Command implements Action {

    private final Command command;

    public HelpCommandCommand(Command command) {
        super("help " + command.getPrefix(), command.getName());
        this.command = command;
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        // ignore any extra parameters
        return this;
    }

    @Override
    public String getHelpSummary() {
        return "Displays information about the `" + this.command.getFullName() + "' command";
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        final PrintWriter out = new PrintWriter(session.getConsole().getOutput());
        out.println(this.command.getFullName() + " - " + this.command.getHelpDetail());
    }
}

