
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class SetSchemaVersionCommand extends AbstractSimpleCommand<Integer> {

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
          + " and `set-allow-new-schema true' has been invoked, then the schema will be recorded under the specified version.";
    }

    @Override
    protected Integer getParameters(Session session, Channels input, ParseContext ctx) {
        final String value = new CommandParser(1, 1, this.getUsage()).parse(ctx).getParams().get(0);
        try {
            final int version = Integer.parseInt(value);
            if (version < 0)
                throw new IllegalArgumentException("schema version is negative");
            return version;
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid schema version `" + value + "'");
        }
    }

    @Override
    protected String getResult(Session session, Channels channels, Integer version) {
        session.setSchemaVersion(version);
        return null;
    }
}

