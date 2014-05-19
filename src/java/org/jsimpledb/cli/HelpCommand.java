
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class HelpCommand extends Command {

    public HelpCommand() {
        super("help command:command?");
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
    protected Parser<?> getParser(String typeName) {
        if ("command".equals(typeName))
            return new CommandParser.CommandNameParser();
        return super.getParser(typeName);
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Command command = (Command)params.get("command");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final PrintWriter writer = session.getWriter();
                if (command == null) {
                    writer.println("Available commands:");
                    for (Command c : CommandParser.getCommands().values())
                        writer.println(String.format("%24s - %s", c.getName(), c.getHelpSummary()));
                } else {
                    writer.println("Usage: " + command.getUsage());
                    writer.println(command.getHelpDetail());
                }
            }
        };
    }
}

