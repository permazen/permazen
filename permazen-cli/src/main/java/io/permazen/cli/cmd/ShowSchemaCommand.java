
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.CliSession;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ParseContext;

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
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Integer version = (Integer)params.get("version");
        return new ShowSchemaAction(version != null ? version : 0);
    }

    private static class ShowSchemaAction implements CliSession.Action {

        private final int version;

        ShowSchemaAction(int version) {
            this.version = version;
        }

        @Override
        public void run(CliSession session) throws Exception {

            // Get schema model
            final SchemaModel schemaModel = AbstractSchemaCommand.getSchemaModel(session, this.version);
            if (schemaModel == null)
                return;

            // Print it out with version (if known)
            if (this.version != 0)
                session.getWriter().println("=== Schema version " + this.version + " ===");
            session.getWriter().println(schemaModel.toString().replaceAll("^<.xml[^>]+>\\n", ""));
        }
    }
}

