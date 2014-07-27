
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.WordParser;
import org.jsimpledb.parse.func.AbstractFunction;

@Command
public class HelpCommand extends AbstractCommand {

    private final CliSession session;

    public HelpCommand(CliSession session) {
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
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String name = (String)params.get("command-or-function");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final PrintWriter writer = session.getWriter();
                if (name == null) {
                    writer.println("Available commands:");
                    for (AbstractCommand availableCommand : session.getCommands().values())
                        writer.println(String.format("%24s - %s", availableCommand.getName(), availableCommand.getHelpSummary()));
                    writer.println("Available functions:");
                    for (AbstractFunction availableFunction : session.getFunctions().values())
                        writer.println(String.format("%24s - %s", availableFunction.getName(), availableFunction.getHelpSummary()));
                } else {
                    final AbstractCommand command = session.getCommands().get(name);
                    if (command != null) {
                        writer.println("Usage: " + command.getUsage());
                        writer.println(command.getHelpDetail());
                    }
                    final AbstractFunction function = session.getFunctions().get(name);
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

