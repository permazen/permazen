
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Map;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.WordParser;
import org.jsimpledb.util.ParseContext;

@CliCommand
public class HelpCommand extends Command {

    private final Session session;

    public HelpCommand(Session session) {
        super("help command-or-function:cmdfunc?");
        this.session = session;
    }

    @Override
    public String getHelpSummary() {
        return "display help information";
    }

    @Override
    public String getHelpDetail() {
        return "Displays the list of known commands and functions, or help information about a specific command or function.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        if ("cmdfunc".equals(typeName)) {
            return new WordParser("command/function") {
                @Override
                protected HashSet<String> getWords() {
                    final HashSet<String> names = new HashSet<>();
                    names.addAll(HelpCommand.this.session.getCommands().keySet());
                    names.addAll(HelpCommand.this.session.getFunctions().keySet());
                    return names;
                }
            };
        }
        return super.getParser(typeName);
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String name = (String)params.get("command-or-function");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final PrintWriter writer = session.getWriter();
                if (name == null) {
                    writer.println("Available commands:");
                    for (Command availableCommand : session.getCommands().values())
                        writer.println(String.format("%24s - %s", availableCommand.getName(), availableCommand.getHelpSummary()));
                    writer.println("Available functions:");
                    for (Function availableFunction : session.getFunctions().values())
                        writer.println(String.format("%24s - %s", availableFunction.getName(), availableFunction.getHelpSummary()));
                } else {
                    final Command command = session.getCommands().get(name);
                    if (command != null) {
                        writer.println("Usage: " + command.getUsage());
                        writer.println(command.getHelpDetail());
                    }
                    final Function function = session.getFunctions().get(name);
                    if (function != null) {
                        writer.println("Usage: " + function.getUsage());
                        writer.println(function.getHelpDetail());
                    }
                    if (command == null && function == null)
                        writer.println("No command or function named `" + name + "' exists.");
                }
            }
        };
    }
}

