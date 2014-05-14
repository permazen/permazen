
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.TreeSet;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;

public class ObjectCommand extends AbstractCommand {

    public ObjectCommand() {
        super("object");
    }

    @Override
    public String getUsage() {
        return this.name + " id ...";
    }

    @Override
    public String getHelpSummary() {
        return "access specific objects by object ID";
    }

    @Override
    public String getHelpDetail() {
        return "Pushes a channel onto the stack containing the specified objects.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx) {

        // Parse IDs
        final TreeSet<ObjId> ids = new TreeSet<>();
        while (true) {
            ctx.skipWhitespace();
            if (ctx.getInput().matches(";.*") || ctx.isEOF())
                break;
            ids.add(Util.parseObjId(session, ctx, this.getUsage()));
        }
        if (ids.isEmpty()) {
            Util.parseObjId(session, ctx, this.getUsage());
            throw new ParseException(ctx, "Usage: " + this.getUsage());     // should never get here
        }

        // Return instances
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                ObjectCommand.this.push(session, new ObjectChannel(session) {
                    @Override
                    public TreeSet<ObjId> getItems(Session session) {
                        return ids;
                    }
                });
            }
        };
    }
}

