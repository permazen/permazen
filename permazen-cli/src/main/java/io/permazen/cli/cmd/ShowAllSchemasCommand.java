
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.core.Schema;
import io.permazen.core.Transaction;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.util.Map;

public class ShowAllSchemasCommand extends AbstractSchemaCommand {

    public ShowAllSchemasCommand() {
        super("show-all-schemas -x:xml");
    }

    @Override
    public String getHelpSummary() {
        return "Shows all schema versions recorded in the database";
    }

    @Override
    public String getHelpDetail() {
        return "If the `-x' flag is provided, the XML representation of each schema version is included.";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        final boolean xml = params.containsKey("xml");
        return new ShowSchemasAction(xml);
    }

    private static class ShowSchemasAction implements Session.Action {

        private final boolean xml;

        ShowSchemasAction(boolean xml) {
            this.xml = xml;
        }

        @Override
        public void run(Session session) throws Exception {
            AbstractSchemaCommand.runWithoutSchema(session, new SchemaAgnosticAction<Void>() {
                @Override
                public Void runWithoutSchema(Session session, Transaction tx) {
                    for (Map.Entry<SchemaId, Schema> entry : tx.getSchemaBundle().getSchemasBySchemaId().entrySet()) {
                        final SchemaId schemaId = entry.getKey();
                        final SchemaModel model = entry.getValue().getSchemaModel();
                        if (ShowSchemasAction.this.xml) {
                            session.getOutput().println(String.format(
                              "=== Schema \"%s\" ===%n%s",
                              schemaId, model.toString().replaceAll("^<.xml[^>]+>\\n", "")));
                        } else
                            session.getOutput().println(schemaId);
                    }
                    return null;
                }
            });
        }
    }
}
