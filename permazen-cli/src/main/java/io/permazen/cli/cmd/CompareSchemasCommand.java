
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.util.Diffs;

import java.util.EnumSet;
import java.util.Map;

public class CompareSchemasCommand extends AbstractCommand {

    public CompareSchemasCommand() {
        super("compare-schemas schemaId1 schemaId2");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the differences between two schemas recorded in the database";
    }

    @Override
    public String getHelpDetail() {
        return "Finds and displays differences between database schemas and/or the configured schema."
          + " A schema ID of \"-\" means to use the schema configured for this CLI session.";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        final String param1 = (String)params.get("schemaId1");
        final String param2 = (String)params.get("schemaId2");
        final SchemaId schemaId1 = !param1.equals("-") ? new SchemaId(param1) : null;
        final SchemaId schemaId2 = !param2.equals("-") ? new SchemaId(param2) : null;
        return new CompareAction(schemaId1, schemaId2);
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    private static class CompareAction implements Session.Action {

        private final SchemaId schemaId1;
        private final SchemaId schemaId2;

        CompareAction(SchemaId schemaId1, SchemaId schemaId2) {
            this.schemaId1 = schemaId1;
            this.schemaId2 = schemaId2;
        }

        @Override
        public void run(Session session) throws Exception {

            // Get schemas
            final SchemaModel schema1 = SchemaUtil.getSchemaModel(session, this.schemaId1);
            final SchemaModel schema2 = SchemaUtil.getSchemaModel(session, this.schemaId2);
            if (schema1 == null || schema2 == null)
                return;
            final String desc1 = this.schemaId1 == null ?
              "the schema configured on this session" : "schema \"" + this.schemaId1 + "\"";
            final String desc2 = this.schemaId2 == null ?
              "the schema configured on this session" : "schema \"" + this.schemaId2 + "\"";
            final Diffs diffs = schema2.differencesFrom(schema1);
            if (diffs.isEmpty())
                session.getOutput().println("No differences found between " + desc1 + " and " + desc2);
            else
                session.getOutput().println("Found differences found between " + desc1 + " and " + desc2 + "\n" + diffs);
        }
    }
}
