
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Collection;

public class CountCommand extends AbstractTransformChannelCommand<Void> {

    public CountCommand() {
        super("count");
    }

    @Override
    public String getUsage() {
        return this.name;
    }

    @Override
    public String getHelpSummary() {
        return "counts the number of items each each input channel";
    }

    @Override
    public String getHelpDetail() {
        return "The 'count' command counts the number of items each each input channel and outputs each sum as an integer.";
    }

    @Override
    protected <T> Channel<?> transformChannel(final Session session, final Channel<T> channel, Void params) {
        return new SingletonChannel<Integer>(Integer.class) {
            @Override
            public Integer getItem(Session session) {

                // Get items
                final Iterable<? extends T> items = channel.getItems(session);

                // Use Collection.size() if possible
                if (items instanceof Collection)
                    return ((Collection<?>)items).size();

                // Otherwise count items manually
                int count = 0;
                for (T item : items)
                    count++;
                return count;
            }
        };
    }
}

