
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;

public class IterateCommand extends Command {

    public IterateCommand() {
        super("iterate type:type");
    }

    @Override
    public String getHelpSummary() {
        return "iterates objects of a specific type";
    }

    @Override
    public String getHelpDetail() {
        return "The 'iterate' command takes one argument which is the name of an object type."
          + " All instances of that type are then iterated in a new channel pushed on the stack."
          + " The `type' parameter may be a storage ID or the name of a type in the current schema version;"
          + " in the latter case, the name may include an optional `#N' suffix to specify a different schema version N.";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse type
        final int storageId = (Integer)params.get("type");

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

