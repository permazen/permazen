
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.dellroad.stuff.string.ParseContext;

public class SetSchemaVersionCommand extends Command {

    public SetSchemaVersionCommand(AggregateCommand parent) {
        super(parent, "schema-version");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        final String usage = "Usage: " + this.getFullName() + " version";
        final String param = ctx.getInput().trim();
        final int version;
        try {
            version = Integer.parseInt(param);
            if (version < 0)
                throw new IllegalArgumentException("schema version is negative");
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, usage);
        }
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setSchemaVersion(version);
            }
        };
    }

    @Override
    public String getHelpSummary() {
        return "Sets the expected schema version";
    }

    @Override
    public String getHelpDetail() {
        return "Sets the expected schema version version number. If no such schema version is recorded in the database,\n"
          + " and `set allow-new-schema true' has been invoked, then the schema will be recorded under the specified version.";
    }
}

