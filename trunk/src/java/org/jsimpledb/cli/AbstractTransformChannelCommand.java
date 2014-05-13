
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.jsimpledb.util.ParseContext;

/**
 * Support superclass for commands that transform each input channel individually into a transformed output channel.
 */
public abstract class AbstractTransformChannelCommand<P> extends AbstractCommand {

    protected AbstractTransformChannelCommand(String name) {
        super(name);
    }

    @Override
    public Channels parseParameters(Session session, Channels channels, ParseContext ctx) {
        return new Channels(Iterables.transform(channels,
          new TransformChannelFunction(session, this.getParameters(session, channels, ctx))));
    }

    /**
     * Parse the command line.
     *
     * <p>
     * The implementation in {@link AbstractTransformChannelCommand} checks that no command line flags or parameters
     * were given and always returns null.
     * </p>
     *
     * @return parsed command line info
     */
    protected P getParameters(Session session, Channels channels, ParseContext ctx) {
        new CommandParser(0, 0, this.getUsage()).parse(ctx);
        return null;
    }

    /**
     * Transform one channel.
     *
     * @param params parsed command line info
     */
    protected abstract <T> Channel<?> transformChannel(Session session, Channel<T> input, P params);

// TransformChannelFunction

    private class TransformChannelFunction implements Function<Channel<?>, Channel<?>> {

        private final Session session;
        private final P params;

        TransformChannelFunction(Session session, P params) {
            this.session = session;
            this.params = params;
        }

        @Override
        public Channel<?> apply(final Channel<?> channel) {
            return AbstractTransformChannelCommand.this.transformChannel(this.session, channel, this.params);
        }
    }
}

