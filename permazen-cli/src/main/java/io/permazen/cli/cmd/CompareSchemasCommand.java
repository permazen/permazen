
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.util.Map;

import io.permazen.cli.CliSession;
import io.permazen.schema.SchemaModel;
import io.permazen.util.Diffs;
import io.permazen.util.ParseContext;

public class CompareSchemasCommand extends AbstractSchemaCommand {

    public CompareSchemasCommand() {
        super("compare-schemas version1:int version2:int");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the differences between two schema versions recorded in the database";
    }

    @Override
    public String getHelpDetail() {
        return "Finds and displays differences between database schema versions and/or the configured schema."
          + " A version number of zero means to use the schema configured for this CLI session.";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final int version1 = (Integer)params.get("version1");
        final int version2 = (Integer)params.get("version2");
        return new CompareAction(version1, version2);
    }

    private static class CompareAction implements CliSession.Action {

        private final int version1;
        private final int version2;

        CompareAction(int version1, int version2) {
            this.version1 = version1;
            this.version2 = version2;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final SchemaModel schema1 = AbstractSchemaCommand.getSchemaModel(session, this.version1);
            final SchemaModel schema2 = AbstractSchemaCommand.getSchemaModel(session, this.version2);
            if (schema1 == null || schema2 == null)
                return;
            final String desc1 = this.version1 == 0 ? "the schema configured on this session" : "schema version " + this.version1;
            final String desc2 = this.version2 == 0 ? "the schema configured on this session" : "schema version " + this.version2;
            final Diffs diffs = schema2.differencesFrom(schema1);
            if (diffs.isEmpty())
                session.getWriter().println("No differences found between " + desc1 + " and " + desc2);
            else
                session.getWriter().println("Found differences found between " + desc1 + " and " + desc2 + "\n" + diffs);
        }
    }
}

