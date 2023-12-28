
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.util.Map;
import java.util.Optional;

public class ShowSchemaCommand extends AbstractSchemaCommand {

    public ShowSchemaCommand() {
        super("show-schema schemaId?");
    }

    @Override
    public String getHelpSummary() {
        return "Shows a specific schema version, or the currently active database schema, in XML form";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        final SchemaId schemaId = Optional.ofNullable((String)params.get("schemaId")).map(SchemaId::new).orElse(null);
        return new ShowSchemaAction(schemaId);
    }

    private static class ShowSchemaAction implements Session.Action {

        private final SchemaId schemaId;

        ShowSchemaAction(SchemaId schemaId) {
            this.schemaId = schemaId;
        }

        @Override
        public void run(Session session) throws Exception {

            // Get schema model
            final SchemaModel schemaModel = AbstractSchemaCommand.getSchemaModel(session, this.schemaId);
            if (schemaModel == null)
                return;

            // Print it out with version (if known)
            if (this.schemaId != null)
                session.getOutput().println(String.format("=== Schema version \"%s\" ===", schemaId));
            session.getOutput().println(schemaModel.toString().replaceAll("^<.xml[^>]+>\\n", ""));
        }
    }
}
