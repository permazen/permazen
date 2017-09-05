
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.CliSession;
import io.permazen.util.ParseContext;

import java.util.Map;

public class SetAllowNewSchemaCommand extends AbstractCommand {

    public SetAllowNewSchemaCommand() {
        super("set-allow-new-schema allowed:boolean");
    }

    @Override
    public String getHelpSummary() {
        return "Sets whether recording a new schema version into the database is allowed";
    }

    @Override
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean allowed = (Boolean)params.get("allowed");
        return session -> {
            session.setAllowNewSchema(allowed);
            session.getWriter().println("Set allow new schema to " + allowed);
        };
    }
}

