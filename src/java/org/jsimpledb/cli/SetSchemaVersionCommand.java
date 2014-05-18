
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
        super("set-schema-version");
    }

    @Override
    public String getUsage() {
        return this.name + " version";
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
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {
        final Map<String, Object> params = new ParamParser(this, "version:int").parseParameters(session, ctx, complete);
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

