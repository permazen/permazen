
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public class SetCommand extends AggregateCommand {

    public SetCommand(AggregateCommand parent) {
        super(parent, "set");
        this.getSubCommands().add(new SetAllowNewSchemaCommand(this));
        this.getSubCommands().add(new SetSchemaVersionCommand(this));
    }

    @Override
    public String getHelpSummary() {
        return "Sets various settings and configuration";
    }
}

