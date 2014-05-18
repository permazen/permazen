
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterables;

import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class LimitCommand extends Command {

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
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {

        // Parse parameters
        final Map<String, Object> params = new ParamParser(this, "p1:int p2:int?").parseParameters(session, ctx, complete);
        final int offset = params.containsKey("p2") ? (Integer)params.get("p1") : 0;
        if (offset < 0)
            throw new ParseException(ctx, "invalid negative offset");
        final int limit = (Integer)params.get(params.containsKey("p2") ? "p2" : "p1");
        if (limit < 0)
            throw new ParseException(ctx, "invalid negative limit");

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

