
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.NavigableSet;
import java.util.TreeSet;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class CreateCommand extends AbstractCommand {

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
    public Action parseParameters(Session session, ParseContext ctx) {

        // Parse options
        final ParamParser parser = new ParamParser(1, 1, this.getUsage(), "-c@", "-v@").parse(ctx);
        final String vflag = parser.getFlag("-v");
        final int version;
        if (vflag != null) {
            try {
                version = Integer.parseInt(vflag);
                if (version < 0)
                    throw new IllegalArgumentException("version is negative");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid version `" + vflag + "'");
            }
        } else
            version = 0;
        final String cflag = parser.getFlag("-c");
        final int count;
        if (cflag != null) {
            try {
                count = Integer.parseInt(vflag);
                if (count < 0)
                    throw new IllegalArgumentException("count is negative");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid count `" + cflag + "'");
            }
        } else
            count = 1;

        // Parse type
        final int storageId = Util.parseTypeName(session, ctx, parser.getParam(0));

        // Return action that creates instance(s) and pushes them onto the stack
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                CreateCommand.this.push(session, new ObjectChannel(session) {
                    @Override
                    public NavigableSet<ObjId> getItems(Session session) {
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

