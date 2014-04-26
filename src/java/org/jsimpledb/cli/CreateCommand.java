
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.SchemaObject;

public class CreateCommand extends Command {

    public CreateCommand(AggregateCommand parent) {
        super(parent, "create");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        final SchemaObject schemaObject = Util.parseSchemaObject(session, ctx, this.getFullName() + " type");
        return new TransactionAction() {
            @Override
            public void run(Session session) throws Exception {
                final Transaction tx = session.getTransaction();
                final ObjId id = tx.create(schemaObject.getStorageId());
                session.getConsole().println("Created new object " + id);
            }
        };
    }

    @Override
    public String getHelpSummary() {
        return "Creates a new instance of the specified type";
    }
}

