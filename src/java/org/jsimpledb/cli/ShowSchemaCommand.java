
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.schema.SchemaModel;

public class ShowSchemaCommand extends Command implements Action {

    public ShowSchemaCommand(AggregateCommand parent) {
        super(parent, "schema");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        if (ctx.getInput().length() != 0)
            throw new ParseException(ctx, "the `" + this.getFullName() + "' command does not take any parameters");
        return this;
    }

    @Override
    public String getHelpSummary() {
        return "Shows the currently active database schema";
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        final SchemaModel schemaModel = session.getSchemaModel();
        if (schemaModel == null) {
            session.getConsole().println("No schema is defined yet");
            return;
        }
        if (session.getSchemaVersion() != 0)
            session.getConsole().println("=== Schema version " + session.getSchemaVersion() + " ===");
        session.getConsole().println(schemaModel.toString().replaceAll("^<.xml[^>]+>\\n", ""));
    }
}

