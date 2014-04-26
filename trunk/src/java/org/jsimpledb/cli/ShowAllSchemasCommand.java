
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.core.SchemaVersion;

public class ShowAllSchemasCommand extends Command implements TransactionAction {

    public ShowAllSchemasCommand(AggregateCommand parent) {
        super(parent, "all-schemas");
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
        for (Map.Entry<Integer, SchemaVersion> entry : session.getTransaction().getSchema().getSchemaVersions().entrySet()) {
            session.getConsole().println("=== Schema version " + entry.getKey() + " ===\n"
              + entry.getValue().getSchemaModel().toString().replaceAll("^<.xml[^>]+>\\n", ""));
        }
    }
}

