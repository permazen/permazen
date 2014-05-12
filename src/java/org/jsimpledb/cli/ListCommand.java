
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.SchemaObject;
import org.jsimpledb.util.ParseContext;

public class ListCommand extends Command {

    public ListCommand(AggregateCommand parent) {
        super(parent, "list");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        final SchemaObject schemaObject = Util.parseSchemaObject(session, ctx, this.getFullName() + " type");
        return new TransactionAction() {
            @Override
            public void run(Session session) throws Exception {
                final Transaction tx = session.getTransaction();
                int count = 0;
                for (ObjId id : tx.getAll(schemaObject.getStorageId())) {
                    if (count >= session.getLineLimit()) {
                        session.getConsole().println("...");
                        break;
                    }
                    final Util.ObjInfo info = Util.getObjInfo(tx, id);
                    session.getConsole().println(info != null ? info.toString() : id + " DELETED?");
                    count++;
                }
            }
        };
    }

    @Override
    public String getHelpSummary() {
        return "Lists objects of a given type";
    }
}

