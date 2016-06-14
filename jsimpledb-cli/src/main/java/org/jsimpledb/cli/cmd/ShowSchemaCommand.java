
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ParseContext;

public class ShowSchemaCommand extends AbstractCommand implements CliSession.Action {

    public ShowSchemaCommand() {
        super("show-schema");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the currently active database schema in XML form";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// CliSession.Action

    @Override
    public void run(CliSession session) throws Exception {

        // Get schema model
        final SchemaModel schemaModel = InfoCommand.getSchemaModel(session);
        if (schemaModel == null) {
            session.getWriter().println("No schema is defined yet");
            return;
        }

        // Print it out with version (if known)
        final int schemaVersion = InfoCommand.getSchemaVersion(session);
        if (schemaVersion != 0)
            session.getWriter().println("=== Schema version " + schemaVersion + " ===");
        session.getWriter().println(schemaModel.toString().replaceAll("^<.xml[^>]+>\\n", ""));
    }
}

