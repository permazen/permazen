
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.schema.SchemaId;

import java.util.Map;

public class DeleteSchemaVersionCommand extends AbstractCommand {

    public DeleteSchemaVersionCommand() {
        super("delete-schema-version schemaId");
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
        return new DeleteSchemaAction(new SchemaId((String)params.get("schemaId")));
    }

    private static class DeleteSchemaAction implements Session.TransactionalAction {

        private final SchemaId schemaId;

        DeleteSchemaAction(SchemaId schemaId) {
            this.schemaId = schemaId;
        }

        @Override
        public void run(Session session) throws Exception {
            final boolean deleted = session.getTransaction().removeSchema(this.schemaId);
            session.getOutput().println("Schema version " + this.schemaId + " " + (deleted ? "deleted" : "not found"));
        }
    }
}
