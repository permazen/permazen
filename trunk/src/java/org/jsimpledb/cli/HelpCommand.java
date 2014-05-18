
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.util.Map;
import java.util.SortedMap;

import org.jsimpledb.util.ParseContext;

public class HelpCommand extends Command {

    public HelpCommand() {
        super("help");
    }

    @Override
    public String getUsage() {
        return this.name + " [command]";
    }

    @Override
    public String getHelpSummary() {
        return "display help information";
    }

    @Override
    public String getHelpDetail() {
        return "Displays help information about a command.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {
        final Map<String, Object> params = new ParamParser(this, "command?").parseParameters(session, ctx, complete);

        // Get all commands
        final SortedMap<String, Command> commandMap = CommandParser.getCommands();

        // Find command specified, if any
        final String commandName = (String)params.get("command");
        final Command command;
        if (commandName == null)
            command = null;
        else if ((command = commandMap.get(commandName)) == null) {
            throw new ParseException(ctx, "unknown command `" + commandName + "'")
              .addCompletions(Util.complete(commandMap.keySet(), commandName));
        }

        // Return help action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final PrintWriter writer = session.getWriter();
                if (command == null) {
                    writer.println("Available commands:");
                    for (Command c : commandMap.values())
                        writer.println(String.format("%24s - %s", c.getName(), c.getHelpSummary()));
                } else {
                    writer.println("Usage: " + command.getUsage());
                    writer.println(command.getHelpDetail());
                }
            }
        };
    }
}

