
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.core.TransactionConfig;
import io.permazen.schema.SchemaId;

import java.util.Map;

public class RemoveSchemaCommand extends AbstractCommand {

    public RemoveSchemaCommand() {
        super("remove-schema schemaId");
    }

    @Override
    public String getHelpSummary() {
        return "Removes the specified schema version from the database";
    }

    @Override
    public String getHelpDetail() {
        return "This command removes a recorded schema from the database. The schema version must not be the"
          + " currently configured schema, and there must not be any objects in the schema remaining in the database";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        return new RemoveSchemaAction(new SchemaId((String)params.get("schemaId")));
    }

    private static class RemoveSchemaAction implements Session.RetryableTransactionalAction {

        private final SchemaId schemaId;

        RemoveSchemaAction(SchemaId schemaId) {
            this.schemaId = schemaId;
        }

        @Override
        public SessionMode getTransactionMode(Session session) {
            return SessionMode.CORE_API;
        }

        @Override
        public TransactionConfig getTransactionConfig(Session session) {
            return Session.RetryableTransactionalAction.super.getTransactionConfig(session).copy()
              .schemaRemoval(TransactionConfig.SchemaRemoval.NEVER)
              .schemaModel(null)
              .build();
        }

        @Override
        public void run(Session session) throws Exception {
            final boolean removed = session.getTransaction().removeSchema(this.schemaId);
            session.getOutput().println("Schema version " + this.schemaId + " " + (removed ? "removed" : "not found"));
        }
    }
}
