
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.dellroad.stuff.string.ParseContext;

public class QuitCommand extends Command implements Action {

    public QuitCommand(AggregateCommand parent) {
        super(parent, "quit");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        if (ctx.getInput().length() != 0)
            throw new ParseException(ctx, "the `" + this.getName() + "' command does not take any parameters");
        return this;
    }

    @Override
    public String getHelpSummary() {
        return "Quits out of the JSimpleDB command line";
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        session.getConsole().println("Bye");
        session.setDone(true);
    }
}

