
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

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
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        session.setDone(true);
        session.getWriter().println("Bye");
    }
}

