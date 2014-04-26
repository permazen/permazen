
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterables;

import java.util.Arrays;

import org.dellroad.stuff.string.ParseContext;

public class SetAllowNewSchemaCommand extends Command {

    public SetAllowNewSchemaCommand(AggregateCommand parent) {
        super(parent, "allow-new-schema");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        final String usage = "Usage: " + this.getFullName() + " [ true | false ]";
        final boolean value;
        try {
            value = Boolean.valueOf(ctx.matchPrefix("(true|false)\\s*$").group(1));
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, usage).addCompletions(
              Iterables.filter(Arrays.asList("true", "false"), new PrefixPredicate(ctx.getInput())));
        }
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setAllowNewSchema(value);
            }
        };
    }

    @Override
    public String getHelpSummary() {
        return "Sets whether recording a new schema version into the database is allowed";
    }
}

