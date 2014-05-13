
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.SortedMap;

import org.jsimpledb.util.ParseContext;

public class HelpCommand extends AbstractSimpleCommand<String> {

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
    protected String getParameters(Session session, Channels input, ParseContext ctx) {
        this.checkChannelCount(input, ctx, 0);

        // Parse help command
        final CommandParser parser = new CommandParser(0, 1, this.getUsage()).parse(ctx);

        // Get commands
        final SortedMap<String, AbstractCommand> commandMap = Command.getCommands();

        // Setup buffer
        final StringWriter buf = new StringWriter();
        final PrintWriter writer = new PrintWriter(buf);

        // Get command name param, if any
        if (parser.getParams().isEmpty()) {
            writer.println("Available commands:");
            for (AbstractCommand command : commandMap.values())
                writer.println(String.format("%24s - %s", command.getName(), command.getHelpSummary()));
        } else {

            // Find specified command
            final String commandName = parser.getParam(0);
            final AbstractCommand command = commandMap.get(commandName);
            if (command == null) {
                throw new ParseException(ctx, "unknown command `" + commandName + "'")
                  .addCompletions(Util.complete(commandMap.keySet(), commandName, false));
            }
            writer.println("Usage: " + command.getUsage());
            writer.println(command.getHelpDetail());
        }

        // Done
        writer.flush();
        return buf.toString().trim();
    }

    @Override
    protected String getResult(Session session, Channels channels, String text) {
        return text;
    }
}

