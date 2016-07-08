
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.core.Schema;
import org.jsimpledb.util.Diffs;
import org.jsimpledb.util.ParseContext;

public class CompareSchemasCommand extends AbstractCommand {

    public CompareSchemasCommand() {
        super("compare-schemas version1:int version2:int");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the differences between two schema versions recorded in the database";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final int version1 = (Integer)params.get("version1");
        final int version2 = (Integer)params.get("version2");
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                final Schema schema1 = session.getTransaction().getSchemas().getVersions().get(version1);
                if (schema1 == null)
                    session.getWriter().println("Schema version " + version1 + " " + "not found");
                final Schema schema2 = session.getTransaction().getSchemas().getVersions().get(version2);
                if (schema2 == null)
                    session.getWriter().println("Schema version " + version2 + " " + "not found");
                if (schema1 == null || schema2 == null)
                    return;
                final Diffs diffs = schema2.getSchemaModel().differencesFrom(schema1.getSchemaModel());
                if (diffs.isEmpty())
                    session.getWriter().println("No differences found between schema versions " + version1 + " and " + version2);
                else {
                    session.getWriter().println("Found differences found between schema versions " + version1 + " and "
                      + version2 + "\n" + diffs);
                }
            }
        };
    }
}

