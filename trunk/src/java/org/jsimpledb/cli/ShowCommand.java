
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public class ShowCommand extends AggregateCommand {

    public ShowCommand(AggregateCommand parent) {
        super(parent, "show");
        this.getSubCommands().add(new ShowSchemaCommand(this));
        this.getSubCommands().add(new ShowAllSchemasCommand(this));
    }

    @Override
    public String getHelpSummary() {
        return "Shows various settings and information";
    }
}

