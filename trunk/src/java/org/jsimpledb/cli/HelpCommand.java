
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;

import org.dellroad.stuff.string.ParseContext;

/**
 * Help command for an {@link AggregateCommand}.
 */
public class HelpCommand extends AggregateCommand implements Action {

    private final AggregateCommand target;

    public HelpCommand(RootCommand target) {
        super("help");
        this.target = target;
    }

    public HelpCommand(AggregateCommand target) {
        super("help" + (target.getPrefix() != null ? " " + target.getPrefix() : ""), target.getName());
        this.target = target;
    }

    public void addSubCommands() {
        this.getSubCommands().clear();
        for (Command command : this.target.getSubCommands()) {
            final Command helpCommand;
            if (command instanceof HelpCommand)
                continue;
            if (command instanceof AggregateCommand)
                helpCommand = new HelpCommand((AggregateCommand)command);
            else if (!command.getHelpDetail().equals(command.getHelpSummary()))
                helpCommand = new HelpCommandCommand(command);
            else
                continue;
            this.getSubCommands().add(helpCommand);
        }
        for (Command command : this.getSubCommands()) {
            if (command instanceof HelpCommand)
                ((HelpCommand)command).addSubCommands();
        }
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        if (ctx.getInput().length() != 0)
            return super.parse(session, ctx);
        return this;
    }

    @Override
    public String getHelpSummary() {
        return this.getPrefix() == null ?
          "Displays information about the specified command" :
          "Displays information about the `" + this.getName() + "' command";
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        final PrintWriter out = new PrintWriter(session.getConsole().getOutput());
        out.println("Available commands:");
        for (Command command : this.target.getSubCommands())
            out.println(String.format("%8s - %s", command.getFullName(), command.getHelpSummary()));
        if (!this.getSubCommands().isEmpty()) {
            out.println("Additional topics available under `" + this.getFullName() + "':");
            for (Command command : this.getSubCommands())
                out.println("    " + command.getFullName());
        }
    }
}

