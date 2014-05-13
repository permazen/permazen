
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public abstract class SingleChannelCommand extends AbstractCommand {

    protected SingleChannelCommand(String name) {
        super(name);
    }

    public final Channels parseParameters(Session session, Channels channels, ParseContext ctx) {
        if (channels.size() != 1) {
            throw new RuntimeException("the `" + this.name + "' command expects a single channel but "
              + channels.size() + " were given");
        }
        return new Channels(this.parseParameters(session, channels.get(0), ctx));
    }

    protected abstract <T> Channel<?> parseParameters(Session session, Channel<T> channel, ParseContext ctx);
}

