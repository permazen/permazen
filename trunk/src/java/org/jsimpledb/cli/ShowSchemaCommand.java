
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.jsimpledb.schema.SchemaModel;

public class ShowSchemaCommand extends AbstractSimpleCommand<Void> {

    public ShowSchemaCommand() {
        super("show-schema");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the currently active database schema";
    }

    @Override
    protected String getResult(Session session, Channels channels, Void params) {

        // Get schema model
        final SchemaModel schemaModel = session.getSchemaModel();
        if (schemaModel == null)
            return "No schema is defined yet";

        // Print it out
        final StringWriter buf = new StringWriter();
        final PrintWriter writer = new PrintWriter(buf);
        if (session.getSchemaVersion() != 0)
            writer.println("=== Schema version " + session.getSchemaVersion() + " ===");
        writer.println(schemaModel.toString().replaceAll("^<.xml[^>]+>\\n", ""));

        // Done
        writer.flush();
        return buf.toString();
    }
}

