
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;

@Command
public class DeleteSchemaVersionCommand extends AbstractCommand {

    public DeleteSchemaVersionCommand() {
        super("delete-schema-version version:int");
    }

    @Override
    public String getHelpSummary() {
        return "Deletes the specified schema version from the database";
    }

    @Override
    public String getHelpDetail() {
        return "This command deletes a schema version recorded in the database. The schema version must not be the"
          + " currently configured schema version and there must not be any objects having that version in the database";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final int version = (Integer)params.get("version");
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                final boolean deleted = session.getTransaction().deleteSchemaVersion(version);
                session.getWriter().println("Schema version " + version + " " + (deleted ? "deleted" : "not found"));
            }
        };
    }
}

