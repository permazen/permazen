
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterables;

import org.jsimpledb.util.ParseContext;

public class LimitCommand extends AbstractCommand {

    public LimitCommand() {
        super("limit");
    }

    @Override
    public String getUsage() {
        return this.name + " [offset] count";
    }

    @Override
    public String getHelpSummary() {
        return "discards items beyond a maximum count, and optionally prior to a minimum offset";
    }

    @Override
    public String getHelpDetail() {
        return "Truncates incoming data items after the specified number have been copied. If an offset is specified,"
          + " data items prior to the offset are skipped.";
    }

    @Override
    public Channels parseParameters(Session session, Channels channels, ParseContext ctx) {
        final CommandParser parser = new CommandParser(1, 2, this.getUsage()).parse(ctx);
        final String offsetParam = parser.getParam(0);
        int offset = 0;
        if (parser.getParams().size() > 1) {
            try {
                offset = Integer.parseInt(parser.getParam(0));
                if (offset < 0)
                    throw new IllegalArgumentException("offset is negative");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid offset `" + offsetParam + "'");
            }
        }
        final String limitParam = parser.getParam(parser.getParams().size() == 1 ? 0 : 1);
        final int limit;
        try {
            limit = Integer.parseInt(limitParam);
            if (limit < 0)
                throw new IllegalArgumentException("limit is negative");
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid limit `" + limitParam + "'");
        }
        final Channels output = new Channels();
        for (Channel<?> channel : channels)
            output.add(this.limitChannel(session, channel, offset, limit));
        return output;
    }

    private <T> Channel<T> limitChannel(Session session, final Channel<T> channel, final int offset, final int limit) {
        return new AbstractChannel<T>(channel.getItemType()) {
            @Override
            public Iterable<T> getItems(Session session) {
                return Iterables.limit(Iterables.skip(channel.getItems(session), offset), limit);
            }
        };
    }
}

