
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class DeleteCommand extends Command {

    public DeleteCommand(AggregateCommand parent) {
        super(parent, "delete");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        final ObjId id = Util.parseObjId(session, ctx, this.getFullName() + " object-id");
        return new TransactionAction() {
            @Override
            public void run(Session session) throws Exception {
                final Transaction tx = session.getTransaction();
                if (tx.delete(id))
                    session.getConsole().println("Deleted object " + id);
                else
                    session.getConsole().println("Object " + id + " does not exist");
            }
        };
    }

    @Override
    public String getHelpSummary() {
        return "Deletes an object instance";
    }
}

