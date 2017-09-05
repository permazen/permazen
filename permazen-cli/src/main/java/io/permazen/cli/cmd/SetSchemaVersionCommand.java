
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.util.Map;

import io.permazen.cli.CliSession;
import io.permazen.parse.ParseException;
import io.permazen.util.ParseContext;

public class SetSchemaVersionCommand extends AbstractCommand {

    public SetSchemaVersionCommand() {
        super("set-schema-version version:int");
    }

    @Override
    public String getHelpSummary() {
        return "Sets the expected schema version";
    }

    @Override
    public String getHelpDetail() {
        return "Sets the expected schema version version number. If no such schema version is recorded in the database,"
          + " and `set-allow-new-schema true' has been invoked, then the current schema will be recorded anew under the"
          + " specified version number.";
    }

    @Override
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final int version = (Integer)params.get("version");
        if (version < -1)
            throw new ParseException(ctx, "invalid negative schema version");
        return session -> {
            session.setSchemaVersion(version);
            session.getWriter().println("Set schema version to " + version);
        };
    }
}

