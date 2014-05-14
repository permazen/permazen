
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
        return "Truncates data items in the top channel after the specified number have been copied. If an offset is specified,"
          + " that many data items are skipped over first.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx) {

        // Parse parameters
        final ParamParser parser = new ParamParser(1, 2, this.getUsage()).parse(ctx);
        final String offsetParam = parser.getParam(0);
        final int offset;
        if (parser.getParams().size() > 1) {
            try {
                offset = Integer.parseInt(offsetParam);
                if (offset < 0)
                    throw new IllegalArgumentException("offset is negative");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid offset `" + offsetParam + "'");
            }
        } else
            offset = 0;
        final String limitParam = parser.getParam(parser.getParams().size() == 1 ? 0 : 1);
        final int limit;
        try {
            limit = Integer.parseInt(limitParam);
            if (limit < 0)
                throw new IllegalArgumentException("limit is negative");
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid limit `" + limitParam + "'");
        }

        // Return action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                LimitCommand.this.limit(session, LimitCommand.this.pop(session), offset, limit);
            }
        };
    }

    // This method exists solely to bind the generic type parameters
    private <T> void limit(Session session, final Channel<T> channel, final int offset, final int limit) throws Exception {
        LimitCommand.this.push(session, new AbstractChannel<T>(channel.getItemType()) {
            @Override
            public Iterable<T> getItems(Session session) {
                return Iterables.limit(Iterables.skip(channel.getItems(session), offset), limit);
            }
        });
    }
}

