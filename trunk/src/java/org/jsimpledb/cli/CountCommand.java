
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Collection;
import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class CountCommand extends Command implements Action {

    public CountCommand() {
        super("count");
    }

    @Override
    public String getHelpSummary() {
        return "counts the number of items in the top input channel";
    }

    @Override
    public String getHelpDetail() {
        return "The `" + this.getName() + "' command replaces the input channel on the top of the stack"
        + " with the the number of items in that channel.";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return this;
    }

// Action

    @Override
    public void run(Session session) throws Exception {

        // Get items
        final Iterable<?> items = this.pop(session).getItems(session);

        // Use Collection.size() if possible, ptherwise count items manually
        int count;
        if (items instanceof Collection)
            count = ((Collection<?>)items).size();
        else {
            count = 0;
            for (Object item : items)
                count++;
        }

        // Push result
        this.push(session, Integer.class, count);
    }
}

