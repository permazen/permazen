
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

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
          + " All instances of that type are then iterated in a new channel pushed on the stack.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx) {

        // Parse type
        final ParamParser parser = new ParamParser(1, 1, this.getUsage()).parse(ctx);
        final int storageId = Util.parseTypeName(session, ctx, parser.getParam(0));

        // Return all instances
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                IterateCommand.this.push(session, new ObjectChannel(session) {
                    @Override
                    public Iterable<ObjId> getItems(Session session) {
                        return session.getTransaction().getAll(storageId);
                    }
                });
            }
        };
    }
}

