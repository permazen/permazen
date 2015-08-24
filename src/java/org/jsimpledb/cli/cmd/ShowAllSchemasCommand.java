
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.core.Schema;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.schema.SchemaModel;

@Command
public class ShowAllSchemasCommand extends AbstractCommand {

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
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean xml = params.containsKey("xml");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                for (Map.Entry<Integer, Schema> entry : session.getTransaction().getSchemas().getVersions().entrySet()) {
                    final int number = entry.getKey();
                    final SchemaModel model = entry.getValue().getSchemaModel();
                    if (xml) {
                        session.getWriter().println("=== Schema version " + number + " ===\n"
                          + model.toString().replaceAll("^<.xml[^>]+>\\n", ""));
                    } else
                        session.getWriter().println(number);
                }
            }
        };
    }
}

