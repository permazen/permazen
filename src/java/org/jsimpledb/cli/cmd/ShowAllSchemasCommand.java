
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.util.ParseContext;

@CliCommand
public class ShowAllSchemasCommand extends Command implements Action {

    public ShowAllSchemasCommand() {
        super("show-all-schemas");
    }

    @Override
    public String getHelpSummary() {
        return "Shows all schema versions recorded in the database";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        for (Map.Entry<Integer, SchemaVersion> entry : session.getTransaction().getSchema().getSchemaVersions().entrySet()) {
            session.getWriter().println("=== Schema version " + entry.getKey() + " ===\n"
              + entry.getValue().getSchemaModel().toString().replaceAll("^<.xml[^>]+>\\n", ""));
        }
    }
}

