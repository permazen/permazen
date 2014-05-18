
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class QuitCommand extends Command implements Action {

    public QuitCommand() {
        super("quit");
    }

    @Override
    public String getHelpSummary() {
        return "Quits out of the JSimpleDB command line";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {
        new ParamParser(this).parseParameters(session, ctx, complete);
        return this;
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        session.setDone(true);
        session.getWriter().println("Bye");
    }
}

