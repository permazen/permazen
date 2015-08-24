
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;

@Command
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
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final int version = (Integer)params.get("version");
        if (version < 0)
            throw new ParseException(ctx, "invalid negative schema version");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                session.setSchemaVersion(version);
                session.getWriter().println("Set schema version to " + version);
            }
        };
    }
}

