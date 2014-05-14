
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.util.SortedMap;

import org.jsimpledb.util.ParseContext;

public class HelpCommand extends AbstractCommand {

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
    public Action parseParameters(Session session, ParseContext ctx) {
        final ParamParser parser = new ParamParser(0, 1, this.getUsage()).parse(ctx);

        // Get all commands
        final SortedMap<String, AbstractCommand> commandMap = CommandParser.getCommands();

        // Find command specified, if any
        final AbstractCommand command;
        if (parser.getParams().isEmpty())
            command = null;
        else {
            final String commandName = parser.getParam(0);
            if ((command = commandMap.get(commandName)) == null) {
                throw new ParseException(ctx, "unknown command `" + commandName + "'")
                  .addCompletions(Util.complete(commandMap.keySet(), commandName, false));
            }
        }

        // Return help action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final PrintWriter writer = session.getWriter();
                if (command == null) {
                    writer.println("Available commands:");
                    for (AbstractCommand c : commandMap.values())
                        writer.println(String.format("%24s - %s", c.getName(), c.getHelpSummary()));
                } else {
                    writer.println("Usage: " + command.getUsage());
                    writer.println(command.getHelpDetail());
                }
            }
        };
    }
}

