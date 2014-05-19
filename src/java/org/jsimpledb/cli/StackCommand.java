
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class StackCommand extends Command implements Action {

    public StackCommand() {
        super("stack");
    }

    @Override
    public String getHelpSummary() {
        return "Shows the current channel stack";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// Action

    @Override
    public void run(Session session) throws Exception {
        int depth = 0;
        for (Channel<?> channel : session.getStack())
            this.print(session, depth++, channel);
    }

    // This method exists solely to bind the generic type parameters
    private <T> void print(Session session, int depth, Channel<T> channel) {
        final PrintWriter writer = session.getWriter();
        writer.print("[" + depth + "] ");
        final Iterator<T> i = channel.getItems(session).iterator();
        if (i.hasNext())
            channel.getItemType().print(session, i.next(), false);
        else
            writer.println("empty");
    }
}

