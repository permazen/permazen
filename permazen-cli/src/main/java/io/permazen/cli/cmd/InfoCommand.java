
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.ValidationMode;
import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.encoding.EncodingRegistry;
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

        // KEY_VALUE mode
        writer.println(String.format("  Database: %s", session.getDatabaseDescription()));
        writer.println(String.format("  Session Mode: %s", session.getMode()));
        writer.println(String.format("  Access: %s", session.isReadOnly() ? "Read-Only" : "Read/Write"));
        writer.println(String.format("  Verbose Mode: %s", session.isVerbose()));
        if (session.getMode().equals(SessionMode.KEY_VALUE))
            return;

        // CORE_API mode
        final EncodingRegistry encodingRegistry = session.getDatabase().getEncodingRegistry();
        writer.println(String.format("  Encoding Registry: %s", encodingRegistry.getClass().getName()));
        final SchemaId schemaId = InfoCommand.getSchemaId(session);
        writer.println(String.format("  Schema ID: %s", schemaId != null ? schemaId : "Undefined"));
        final SchemaModel schemaModel = InfoCommand.getSchemaModel(session);
        writer.println(String.format("  Schema Model: %s",
          schemaModel != null ?
            (schemaModel.isEmpty() ? "Empty" : schemaModel.getSchemaObjectTypes().size() + " object type(s)") :
            "Undefined"));
        writer.println(String.format("  Schema Removal: %s", session.getSchemaRemoval()));
        writer.println(String.format("  New Schema Allowed: %s", session.isAllowNewSchema() ? "Yes" : "No"));

        // PERMAZEN mode
        if (session.getPermazen() != null) {
            writer.println(String.format("  Validation Mode: %s", session.getValidationMode() != null ?
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
