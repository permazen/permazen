
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Arrays;

import org.jsimpledb.util.ParseContext;

public class SetAllowNewSchemaCommand extends AbstractCommand {

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
    public Action parseParameters(Session session, ParseContext ctx) {
        final String value = new ParamParser(1, 1, this.getUsage()).parse(ctx).getParams().get(0);
        if (!value.matches("(?i)true|false")) {
            throw new ParseException(ctx, "invalid value `" + value + "'; specify either `true' or `false'")
              .addCompletions(Util.complete(Arrays.asList("true", "false"), value, false));
        }
        final boolean allow = Boolean.valueOf(value);
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setAllowNewSchema(allow);
                session.getWriter().println("Set allow new schema to " + allow);
            }
        };
    }
}

