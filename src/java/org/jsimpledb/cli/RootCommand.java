
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public class RootCommand extends AggregateCommand {

    public RootCommand() {
        super("");
        final HelpCommand help = new HelpCommand(this);
        this.getSubCommands().add(help);
        this.getSubCommands().add(new CreateCommand(this));
        this.getSubCommands().add(new DeleteCommand(this));
        this.getSubCommands().add(new ListCommand(this));
        this.getSubCommands().add(new PrintCommand(this));
        this.getSubCommands().add(new ShowCommand(this));
        this.getSubCommands().add(new SetCommand(this));
        this.getSubCommands().add(new QuitCommand(this));
        help.addSubCommands();
    }

    @Override
    public String getHelpSummary() {
        throw new UnsupportedOperationException("no help for command root");
    }
}

