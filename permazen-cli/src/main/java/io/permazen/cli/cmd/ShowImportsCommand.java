
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.util.ParseContext;

import java.util.EnumSet;
import java.util.Map;

public class ShowImportsCommand extends AbstractCommand implements CliSession.Action {

    public ShowImportsCommand() {
        super("show-imports");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the Java imports associated with the current CLI session.";
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
        for (String value : session.getImports())
            session.getWriter().println(value);
    }
}

