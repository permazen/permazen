
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.schema.SchemaModel;

import java.util.Map;

public class ShowSchemaCommand extends AbstractSchemaCommand {

    public ShowSchemaCommand() {
        super("show-schema version:int?");
    }

    @Override
    public String getHelpSummary() {
        return "Shows a specific schema version, or the currently active database schema, in XML form";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        final Integer version = (Integer)params.get("version");
        return new ShowSchemaAction(version != null ? version : 0);
    }

    private static class ShowSchemaAction implements Session.Action {

        private final int version;

        ShowSchemaAction(int version) {
            this.version = version;
        }

        @Override
        public void run(Session session) throws Exception {

            // Get schema model
            final SchemaModel schemaModel = AbstractSchemaCommand.getSchemaModel(session, this.version);
            if (schemaModel == null)
                return;

            // Print it out with version (if known)
            if (this.version != 0)
                session.getOutput().println("=== Schema version " + this.version + " ===");
            session.getOutput().println(schemaModel.toString().replaceAll("^<.xml[^>]+>\\n", ""));
        }
    }
}
