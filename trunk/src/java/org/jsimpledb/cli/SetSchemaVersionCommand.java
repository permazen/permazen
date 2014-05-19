
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class SetSchemaVersionCommand extends Command {

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
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final int version = (Integer)params.get("version");
        if (version < 0)
            throw new ParseException(ctx, "invalid negative schema version");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setSchemaVersion(version);
                session.getWriter().println("Set schema version to " + version);
            }
        };
    }
}

