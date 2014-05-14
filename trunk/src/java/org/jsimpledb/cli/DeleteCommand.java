
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class DeleteCommand extends AbstractCommand {

    public DeleteCommand() {
        super("delete");
    }

    @Override
    public String getUsage() {
        return this.name;
    }

    @Override
    public String getHelpSummary() {
        return "delete objects";
    }

    @Override
    public String getHelpDetail() {
        return "Deletes all objects in the input channel on top of the stack.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx) {
        final ParamParser parser = new ParamParser(0, 0, this.getUsage()).parse(ctx);
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final Channel<? extends ObjId> channel = DeleteCommand.this.pop(session, ObjId.class);
                final Transaction tx = session.getTransaction();
                int count = 0;
                for (ObjId id : channel.getItems(session)) {
                    tx.delete(id);
                    count++;
                }
                session.getWriter().println("Deleted " + count + " object" + (count != 1 ? "s" : ""));
            }
        };
    }
}

