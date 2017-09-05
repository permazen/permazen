
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.util.Map;

import io.permazen.Session;
import io.permazen.cli.CliSession;
import io.permazen.util.ParseContext;

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
        return new DeleteSchemaAction((Integer)params.get("version"));
    }

    private static class DeleteSchemaAction implements CliSession.Action, Session.TransactionalAction {

        private final int version;

        DeleteSchemaAction(int version) {
            this.version = version;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final boolean deleted = session.getTransaction().deleteSchemaVersion(this.version);
            session.getWriter().println("Schema version " + this.version + " " + (deleted ? "deleted" : "not found"));
        }
    }
}

