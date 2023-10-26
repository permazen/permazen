
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;

import java.util.Map;

public class SetVerboseCommand extends AbstractCommand {

    public SetVerboseCommand() {
        super("set-verbose verbose:boolean");
    }

    @Override
    public String getHelpSummary() {
        return "Enables or disables verbose mode, which displays the complete stack trace when an exceptions occurs.";
    }

    @Override
    public Session.Action getAction(Session session0, Map<String, Object> params) {
        final boolean verbose = (Boolean)params.get("verbose");
        return session -> {
            session.setVerbose(verbose);
            session.getOutput().println((verbose ? "En" : "Dis") + "abled verbose mode.");
        };
    }
}
