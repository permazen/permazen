
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.EnumSet;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.util.ParseContext;

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

