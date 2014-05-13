
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterables;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.jsimpledb.util.ParseContext;

public class Command implements Parser<Channels> {

    private static final TreeMap<String, AbstractCommand> COMMANDS = new TreeMap<>();

    static {
        Command.registerCommand(new CountCommand());
        Command.registerCommand(new CreateCommand());
        Command.registerCommand(new DeleteCommand());
        Command.registerCommand(new ExportCommand());
        Command.registerCommand(new GetCommand());
        Command.registerCommand(new HelpCommand());
        Command.registerCommand(new ImportCommand());
        Command.registerCommand(new IterateCommand());
        Command.registerCommand(new LimitCommand());
        Command.registerCommand(new PrintCommand());
        Command.registerCommand(new SetAllowNewSchemaCommand());
        Command.registerCommand(new SetSchemaVersionCommand());
        Command.registerCommand(new ShowAllSchemasCommand());
        Command.registerCommand(new ShowSchemaCommand());
    }

    public static SortedMap<String, AbstractCommand> getCommands() {
        return Command.COMMANDS;
    }

    public static void registerCommand(AbstractCommand command) {
        Command.COMMANDS.put(command.getName(), command);
    }

    @Override
    public Channels parse(Session session, Channels input, ParseContext ctx) {

        // Parse command name
        final Matcher matcher = ctx.tryPattern("\\s*([^\\s,|)]+)");
        if (matcher == null) {
            throw new ParseException(ctx, "invalid empty command")
              .addCompletions(Iterables.transform(COMMANDS.keySet(), new AddSuffixFunction(" ")));
        }
        final String commandName = matcher.group(1);

        // Get matching command
        final AbstractCommand command = COMMANDS.get(commandName);
        if (command == null) {
            throw new ParseException(ctx, "unknown command `" + commandName + "'")
              .addCompletions(Util.complete(COMMANDS.keySet(), commandName, false));
        }

        // Try to parse command parameters
        return command.parseParameters(session, input, ctx);
    }
}

