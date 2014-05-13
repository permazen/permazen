
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class SetAllowNewSchemaCommand extends AbstractSimpleCommand<Boolean> {

    public SetAllowNewSchemaCommand() {
        super("set-allow-new-schema");
    }

    @Override
    public String getUsage() {
        return this.name + " [ true | false ]";
    }

    @Override
    public String getHelpSummary() {
        return "Sets whether recording a new schema version into the database is allowed";
    }

    @Override
    protected Boolean getParameters(Session session, Channels input, ParseContext ctx) {
        final String value = new CommandParser(1, 1, this.getUsage()).parse(ctx).getParams().get(0);
        if (value.equalsIgnoreCase("true"))
            return true;
        if (value.equalsIgnoreCase("false"))
            return false;
        throw new ParseException(ctx, "invalid value `" + value + "' for `" + this.name + "' command");
    }

    @Override
    protected String getResult(Session session, Channels channels, Boolean allow) {
        session.setAllowNewSchema(allow);
        return null;
    }
}

