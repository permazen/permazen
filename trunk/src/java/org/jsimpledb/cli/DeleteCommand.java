
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class DeleteCommand extends AbstractTransformChannelCommand<Void> {

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
        return "Deletes all objects found on any input channel.";
    }

    protected Void getParameters(Session session, Channels channels, ParseContext ctx) {
        this.checkItemType(channels, ctx, ObjId.class);
        return super.getParameters(session, channels, ctx);
    }

    @Override
    protected <T> Channel<?> transformChannel(Session session, final Channel<T> input, Void params) {

        // Delete objects
        return new EmptyChannel() {
            @Override
            protected void process(Session session) {
                final Transaction tx = session.getTransaction();
                int count = 0;
                for (T obj : input.getItems(session)) {
                    tx.delete((ObjId)obj);
                    count++;
                }
                session.getWriter().println("Deleted " + count + " object" + (count != 1 ? "s" : ""));
            }
        };
    }
}

