
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.EnumNameParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.Parser;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class SetSessionModeCommand extends AbstractCommand {

    public SetSessionModeCommand() {
        super("set-session-mode mode:mode");
    }

    @Override
    public String getHelpSummary() {
        return "Sets the CLI session mode";
    }

    @Override
    public String getHelpDetail() {
        return "Changes the current CLI session mode. Specify JSIMPLEDB, CORE_API, or KEY_VALUE.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "mode".equals(typeName) ? new EnumNameParser<SessionMode>(SessionMode.class, false) : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final SessionMode mode = (SessionMode)params.get("mode");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                session.setMode(mode);
                session.getWriter().println("Set session mode to " + mode);
            }
        };
    }
}

