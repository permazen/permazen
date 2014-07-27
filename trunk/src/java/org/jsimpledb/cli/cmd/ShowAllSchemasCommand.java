
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.parse.ParseContext;

@Command
public class ShowAllSchemasCommand extends AbstractCommand implements CliSession.Action {

    public ShowAllSchemasCommand() {
        super("show-all-schemas");
    }

    @Override
    public String getHelpSummary() {
        return "Shows all schema versions recorded in the database";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// CliSession.Action

    @Override
    public void run(CliSession session) throws Exception {
        for (Map.Entry<Integer, SchemaVersion> entry : session.getTransaction().getSchema().getSchemaVersions().entrySet()) {
            session.getWriter().println("=== Schema version " + entry.getKey() + " ===\n"
              + entry.getValue().getSchemaModel().toString().replaceAll("^<.xml[^>]+>\\n", ""));
        }
    }
}

