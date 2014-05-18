
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class SetAllowNewSchemaCommand extends Command {

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
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {
        final Map<String, Object> params = new ParamParser(this, "allow:boolean").parseParameters(session, ctx, complete);
        final boolean allow = (Boolean)params.get("allow");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setAllowNewSchema(allow);
                session.getWriter().println("Set allow new schema to " + allow);
            }
        };
    }
}

