
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class SetAllowNewSchemaCommand extends Command {

    public SetAllowNewSchemaCommand() {
        super("set-allow-new-schema allowed:boolean");
    }

    @Override
    public String getHelpSummary() {
        return "Sets whether recording a new schema version into the database is allowed";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean allowed = (Boolean)params.get("allowed");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setAllowNewSchema(allowed);
                session.getWriter().println("Set allow new schema to " + allowed);
            }
        };
    }
}

