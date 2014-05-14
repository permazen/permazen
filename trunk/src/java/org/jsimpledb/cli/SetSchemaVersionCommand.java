
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class SetSchemaVersionCommand extends AbstractCommand {

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
    public Action parseParameters(Session session, ParseContext ctx) {
        final String value = new ParamParser(1, 1, this.getUsage()).parse(ctx).getParam(0);
        final int version;
        try {
            version = Integer.parseInt(value);
            if (version < 0)
                throw new IllegalArgumentException("schema version is negative");
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid schema version `" + value + "'");
        }
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setSchemaVersion(version);
                session.getWriter().println("Set schema version to " + version);
            }
        };
    }
}

