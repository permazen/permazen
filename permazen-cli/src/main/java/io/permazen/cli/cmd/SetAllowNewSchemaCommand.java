
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;

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
    public Session.Action getAction(Session session0, Map<String, Object> params) {
        final boolean allowed = (Boolean)params.get("allowed");
        return session -> {
            session.setAllowNewSchema(allowed);
            session.getOutput().println("Set allow new schema to " + allowed);
        };
    }
}
