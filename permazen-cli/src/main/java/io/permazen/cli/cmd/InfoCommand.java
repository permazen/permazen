
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.ValidationMode;
import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Map;

public class InfoCommand extends AbstractCommand implements Session.Action {

    public InfoCommand() {
        super("info");
    }

    @Override
    public String getHelpSummary() {
        return "Shows general information about the CLI database";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        return this;
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

// Session.Action

    @Override
    public void run(Session session) throws Exception {
        final PrintStream writer = session.getOutput();
        writer.println("  CLI Mode: " + session.getMode());
        writer.println("  Database: " + session.getDatabaseDescription());
        writer.println("  Access Mode: " + (session.isReadOnly() ? "Read-Only" : "Read/Write"));
        writer.println("  Verbose Mode: " + session.isVerbose());
        if (session.getMode().equals(SessionMode.KEY_VALUE))
            return;
        final SchemaId schemaId = InfoCommand.getSchemaId(session);
        writer.println("  Schema ID: " + (schemaId != null ? schemaId : "Undefined"));
        final SchemaModel schemaModel = InfoCommand.getSchemaModel(session);
        writer.println("  Schema Model: " + (schemaModel != null ?
          (schemaModel.isEmpty() ? "Empty" : schemaModel.getSchemaObjectTypes().size() + " object type(s)") :
          "Undefined"));
        writer.println("  New Schema Allowed: " + (session.isAllowNewSchema() ? "Yes" : "No"));
        if (session.getPermazen() != null) {
            writer.println("  Validation Mode: " + (session.getValidationMode() != null ?
              session.getValidationMode() : ValidationMode.AUTOMATIC));
        }
    }

    static SchemaId getSchemaId(Session session) {
        final SchemaModel schemaModel = InfoCommand.getSchemaModel(session);
        return schemaModel != null ? schemaModel.getSchemaId() : null;
    }

    static SchemaModel getSchemaModel(Session session) {
        SchemaModel schemaModel = session.getSchemaModel();
        if (schemaModel == null && session.getPermazen() != null)
            schemaModel = session.getPermazen().getSchemaModel();
        return schemaModel;
    }
}
