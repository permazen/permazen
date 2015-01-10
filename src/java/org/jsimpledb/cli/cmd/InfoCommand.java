
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.Map;

import org.jsimpledb.ValidationMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;

@Command
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

// CliSession.Action

    @Override
    public void run(CliSession session) throws Exception {
        final PrintWriter writer = session.getWriter();
        writer.println("  CLI Mode: " + (session.hasJSimpleDB() ? "JSimpleDB" : "Core API"));
        writer.println("  Database: " + session.getDatabaseDescription());
        writer.println("  Schema Model: " + (session.getSchemaModel() != null ? "defined" : "undefined"));
        writer.println("  Schema Version: " + (session.getSchemaVersion() != 0 ? session.getSchemaVersion() : "undefined"));
        if (session.hasJSimpleDB()) {
            writer.println("  Validation Mode: " + (session.getValidationMode() != null ?
              session.getValidationMode() : ValidationMode.AUTOMATIC));
        }
        writer.println("  New Schema Allowed: " + (session.isAllowNewSchema() ? "yes" : "no"));
        writer.println("  Access Mode: " + (session.isReadOnly() ? "read-only" : "read/write"));
    }
}

