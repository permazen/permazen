
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.EnumSet;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.util.ParseContext;

public class ShowVariablesCommand extends AbstractCommand implements CliSession.Action {

    public ShowVariablesCommand() {
        super("show-variables");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the names and types of all CLI session variables.";
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
        for (Map.Entry<String, Value> entry : session.getVars().entrySet()) {
            final String name = entry.getKey();
            final Class<?> type = entry.getValue().getType(session);
            session.getWriter().println(String.format("$%-20s %s", name, type != null ? type.getName() : "null?"));
        }
    }
}

