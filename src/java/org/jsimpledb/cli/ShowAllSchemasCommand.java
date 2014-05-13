
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.jsimpledb.core.SchemaVersion;

public class ShowAllSchemasCommand extends AbstractSimpleCommand<Void> {

    public ShowAllSchemasCommand() {
        super("show-all-schemas");
    }

    @Override
    public String getHelpSummary() {
        return "Shows all schema versions recorded in the database";
    }

    @Override
    protected String getResult(Session session, Channels channels, Void params) {
        final StringWriter buf = new StringWriter();
        final PrintWriter writer = new PrintWriter(buf);
        for (Map.Entry<Integer, SchemaVersion> entry : session.getTransaction().getSchema().getSchemaVersions().entrySet()) {
            writer.println("=== Schema version " + entry.getKey() + " ===\n"
              + entry.getValue().getSchemaModel().toString().replaceAll("^<.xml[^>]+>\\n", ""));
        }
        writer.flush();
        return buf.toString();
    }
}

