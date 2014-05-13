
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.NavigableSet;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;

public class IterateCommand extends AbstractCommand {

    public IterateCommand() {
        super("iterate");
    }

    @Override
    public String getUsage() {
        return this.name + " type";
    }

    @Override
    public String getHelpSummary() {
        return "iterates objects of a specific type";
    }

    @Override
    public String getHelpDetail() {
        return "The 'iterate' command takes one argument which is the name of an object type."
          + " All instances of that type are then iterated.";
    }

    @Override
    public Channels parseParameters(Session session, Channels input, ParseContext ctx) {
        this.checkChannelCount(input, ctx, 0);
        final CommandParser parser = new CommandParser(1, 1, this.getUsage()).parse(ctx);

        // Parse type
        final int storageId = Util.parseTypeName(session, ctx, parser.getParam(0));

        // Return all instances
        return new Channels(new AbstractChannel<ObjId>(new ObjectItemType()) {
            @Override
            public NavigableSet<ObjId> getItems(Session session) {
                return session.getTransaction().getAll(storageId);
            }
        });
    }
}

