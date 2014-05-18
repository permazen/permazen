
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;
import java.util.TreeSet;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class CreateCommand extends Command {

    public CreateCommand() {
        super("create");
    }

    @Override
    public String getUsage() {
        return this.name + " [-c count] [-v version] type";
    }

    @Override
    public String getHelpSummary() {
        return "create new object instance(s)";
    }

    @Override
    public String getHelpDetail() {
        return "Creates a new instance of the specified type and outputs it. Use `-v' to force a specific object version"
        + " and `-c' to create multiple instances.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {

        // Parse options
        final Map<String, Object> params = new ParamParser(this, "-v:int -c:int type:type").parseParameters(session, ctx, complete);
        final int version = params.containsKey("-v") ? (Integer)params.get("-v") : 0;
        if (version < 0)
            throw new ParseException(ctx, "invalid negative version");
        final int count = params.containsKey("-c") ? (Integer)params.get("-c") : 1;
        if (count < 0)
            throw new ParseException(ctx, "invalid negative count");
        final int storageId = (Integer)params.get("type");

        // Return action that creates instance(s) and pushes them onto the stack
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                CreateCommand.this.push(session, new ObjectChannel(session) {
                    @Override
                    public TreeSet<ObjId> getItems(Session session) {
                        final Transaction tx = session.getTransaction();
                        final TreeSet<ObjId> set = new TreeSet<ObjId>();
                        for (int i = 0; i < count; i++)
                            set.add(version != 0 ? tx.create(storageId, version) : tx.create(storageId));
                        session.getWriter().println("Created " + count + " object" + (count != 1 ? "s" : ""));
                        return set;
                    }
                });
            }
        };
    }
}

