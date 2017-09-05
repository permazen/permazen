
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.io.PrintWriter;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.Parser;
import io.permazen.parse.WordParser;
import io.permazen.parse.func.Function;
import io.permazen.util.ParseContext;

public class HelpCommand extends AbstractCommand {

    private final CliSession session;

    public HelpCommand(CliSession session) {
        super("help -a:all command-or-function:cmdfunc?");
        this.session = session;
    }

    @Override
    public String getHelpSummary() {
        return "Display help information";
    }

    @Override
    public String getHelpDetail() {
        return "Displays the list of known commands and functions, or help information about a specific command or function."
          + "\nNormally only those appropriate for the current session mode are listed; use `-a' to show all.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
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
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean all = params.containsKey("all");
        final String name = (String)params.get("command-or-function");
        final SessionMode sessionMode = session.getMode();
        return session -> {
            final PrintWriter writer = session.getWriter();
            if (name == null) {
                writer.println((all ? "All" : "Available") + " commands:");
                session.getCommands().values().stream()
                  .filter(command -> all || command.getSessionModes().contains(sessionMode))
                  .forEach(command -> writer.println(String.format("%24s - %s", command.getName(), command.getHelpSummary())));
                writer.println((all ? "All" : "Available") + " functions:");
                session.getFunctions().values().stream()
                  .filter(function -> all || function.getSessionModes().contains(sessionMode))
                  .forEach(function -> writer.println(String.format("%24s - %s", function.getName(), function.getHelpSummary())));
            } else {
                final Command command = session.getCommands().get(name);
                if (command != null) {
                    writer.println("Usage: " + command.getUsage());
                    writer.println(command.getHelpDetail());
                    writer.println("Supported session modes: "
                      + command.getSessionModes().toString().replaceAll("\\[(.*)\\]", "$1"));
                }
                final Function function = session.getFunctions().get(name);
                if (function != null) {
                    writer.println("Usage: " + function.getUsage());
                    writer.println(function.getHelpDetail());
                    writer.println("Supported session modes: "
                      + function.getSessionModes().toString().replaceAll("\\[(.*)\\]", "$1"));
                }
                if (command == null && function == null)
                    writer.println("No command or function named `" + name + "' exists.");
            }
        };
    }
}

