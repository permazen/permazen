
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.util.EnumSet;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.util.ParseContext;

public class QuitCommand extends AbstractCommand implements CliSession.Action {

    public QuitCommand() {
        super("quit");
    }

    @Override
    public String getHelpSummary() {
        return "Quits out of the JSimpleDB command line";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// CliSession.Action

    @Override
    public void run(CliSession session) throws Exception {
        session.setDone(true);
        session.getWriter().println("Bye");
    }
}

