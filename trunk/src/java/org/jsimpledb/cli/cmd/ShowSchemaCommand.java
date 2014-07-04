
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ParseContext;

@CliCommand
public class ShowSchemaCommand extends Command implements Action {

    public ShowSchemaCommand() {
        super("show-schema");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the currently active database schema";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// Action

    @Override
    public void run(Session session) throws Exception {

        // Get schema model
        final SchemaModel schemaModel = session.getSchemaModel();
        if (schemaModel == null) {
            session.getWriter().println("No schema is defined yet");
            return;
        }

        // Print it out
        if (session.getSchemaVersion() != 0)
            session.getWriter().println("=== Schema version " + session.getSchemaVersion() + " ===");
        session.getWriter().println(schemaModel.toString().replaceAll("^<.xml[^>]+>\\n", ""));
    }
}

