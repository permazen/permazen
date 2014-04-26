
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

import org.dellroad.stuff.string.ParseContext;

/**
 * A command that consists of sub-commands.
 */
public abstract class AggregateCommand extends Command {

    private final ArrayList<Command> subCommands = new ArrayList<>();

    public AggregateCommand(String name) {
        super(name);
    }

    public AggregateCommand(AggregateCommand parent, String name) {
        super(parent, name);
    }

    public AggregateCommand(String prefix, String name) {
        super(prefix, name);
    }

    public List<Command> getSubCommands() {
        return this.subCommands;
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        String subCommandName = null;
        try {
            subCommandName = ctx.matchPrefix("([^\\s]+)\\s*").group(1);
        } catch (IllegalArgumentException e) {
            // ignore
        }
        if (subCommandName != null) {
            for (Command subCommand : this.getSubCommands()) {
                if (subCommand.getName().equals(subCommandName))
                    return subCommand.parse(session, ctx);
            }
        }

        // Build error message
        String errmsg = "";
        if (this.getName().length() > 0) {
            errmsg += "usage: " + this.getFullName() + " [";
            for (int i = 0; i < this.subCommands.size(); i++) {
                errmsg += i > 0 ? " | " : " ";
                errmsg += this.subCommands.get(i).getName();
            }
            errmsg += " ]";
        } else
            errmsg = "unknown command `" + this.getFullName(subCommandName) + "'; type `help' for help.";

        // Throw exception
        throw new ParseException(ctx, errmsg).addCompletions(Iterables.transform(
          Iterables.filter(Iterables.transform(this.getSubCommands(), Command.NAME_FUNCTION), new PrefixPredicate(subCommandName)),
          new PrefixFunction(subCommandName)));
    }

// PrefixFunction

    private class PrefixFunction implements Function<String, String> {

        private final String prefix;

        PrefixFunction(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String apply(String commandName) {
            return (this.prefix != null ? commandName.substring(prefix.length()) : commandName) + " ";
        }
    }
}

