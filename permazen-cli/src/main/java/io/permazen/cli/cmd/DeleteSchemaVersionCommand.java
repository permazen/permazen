
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;

import java.util.Map;

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
    public Session.Action getAction(Session session, Map<String, Object> params) {
        return new DeleteSchemaAction((Integer)params.get("version"));
    }

    private static class DeleteSchemaAction implements Session.Action, Session.TransactionalAction {

        private final int version;

        DeleteSchemaAction(int version) {
            this.version = version;
        }

        @Override
        public void run(Session session) throws Exception {
            final boolean deleted = session.getTransaction().deleteSchemaVersion(this.version);
            session.getOutput().println("Schema version " + this.version + " " + (deleted ? "deleted" : "not found"));
        }
    }
}

