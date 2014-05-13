
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.NavigableSet;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.NavigableSets;
import org.jsimpledb.util.ParseContext;

public class CreateCommand extends AbstractCommand {

    public CreateCommand() {
        super("create");
    }

    @Override
    public String getUsage() {
        return this.name + " [-v version] type";
    }

    @Override
    public String getHelpSummary() {
        return "creates a new object instance";
    }

    @Override
    public String getHelpDetail() {
        return "Creates a new instance of the specified type and outputs it.";
    }

    @Override
    public Channels parseParameters(Session session, Channels input, ParseContext ctx) {

        // Parse options
        final CommandParser parser = new CommandParser(1, 1, this.getUsage(), "-v@").parse(ctx);
        final String vflag = parser.getFlag("-v");
        final int version;
        if (vflag != null) {
            try {
                version = Integer.parseInt(vflag);
                if (version < 0)
                    throw new IllegalArgumentException("schema version is negative");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid version `" + vflag + "'");
            }
        } else
            version = 0;

        // Parse type
        final int storageId = Util.parseTypeName(session, ctx, parser.getParam(0));

        // Create instance and output it
        return new Channels(new AbstractChannel<ObjId>(new ObjectItemType()) {
            @Override
            public NavigableSet<ObjId> getItems(Session session) {
                final Transaction tx = session.getTransaction();
                final ObjId id = version != 0 ? tx.create(storageId, version) : tx.create(storageId);
                return NavigableSets.<ObjId>singleton(id);
            }
        });
    }
}

