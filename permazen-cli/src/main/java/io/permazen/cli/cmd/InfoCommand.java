
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.io.PrintWriter;
import java.util.EnumSet;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.ValidationMode;
import io.permazen.cli.CliSession;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ParseContext;

public class InfoCommand extends AbstractCommand implements CliSession.Action {

    public InfoCommand() {
        super("info");
    }

    @Override
    public String getHelpSummary() {
        return "Shows general information about the CLI database";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

// CliSession.Action

    @Override
    public void run(CliSession session) throws Exception {
        final PrintWriter writer = session.getWriter();
        writer.println("  CLI Mode: " + session.getMode());
        writer.println("  Database: " + session.getDatabaseDescription());
        writer.println("  Access Mode: " + (session.isReadOnly() ? "Read-Only" : "Read/Write"));
        writer.println("  Verbose Mode: " + session.isVerbose());
        if (session.getMode().equals(SessionMode.KEY_VALUE))
            return;
        final int schemaVersion = InfoCommand.getSchemaVersion(session);
        writer.println("  Schema Version: " + (schemaVersion != 0 ? schemaVersion : "Undefined"));
        final SchemaModel schemaModel = InfoCommand.getSchemaModel(session);
        writer.println("  Schema Model: "
          + (schemaModel != null ? schemaModel.getSchemaObjectTypes().size() + " object type(s)" : "Undefined"));
        writer.println("  New Schema Allowed: " + (session.isAllowNewSchema() ? "Yes" : "No"));
        if (session.getJSimpleDB() != null) {
            writer.println("  Validation Mode: " + (session.getValidationMode() != null ?
              session.getValidationMode() : ValidationMode.AUTOMATIC));
        }
    }

    static int getSchemaVersion(CliSession session) {
        int schemaVersion = session.getSchemaVersion();
        if (schemaVersion == 0 && session.getJSimpleDB() != null) {
            schemaVersion = session.getJSimpleDB().getActualVersion();
            if (schemaVersion == 0)
                schemaVersion = session.getJSimpleDB().getConfiguredVersion();
        }
        return schemaVersion;
    }

    static SchemaModel getSchemaModel(CliSession session) {
        SchemaModel schemaModel = session.getSchemaModel();
        if (schemaModel == null && session.getJSimpleDB() != null)
            schemaModel = session.getJSimpleDB().getSchemaModel();
        return schemaModel;
    }
}

