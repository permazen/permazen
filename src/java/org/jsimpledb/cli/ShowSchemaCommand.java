
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ParseContext;

public class ShowSchemaCommand extends AbstractCommand implements Action {

    public ShowSchemaCommand() {
        super("show-schema");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the currently active database schema";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx) {
        new ParamParser(0, 0, this.getUsage()).parse(ctx);
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

