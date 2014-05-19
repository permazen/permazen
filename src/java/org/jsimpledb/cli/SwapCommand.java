
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class SwapCommand extends Command {

    public SwapCommand() {
        super("swap depth:int?");
    }

    @Override
    public String getHelpSummary() {
        return "swaps the channel(s) on top of the channel stack";
    }

    @Override
    public String getHelpDetail() {
        return "The `swap' command swaps the top two channels on the channel stack. If a `depth'"
         + " other than `1' (the default) is given, that many channels on top of the stack are swapped as a group"
         + " with the same number of channels just below them.";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final int depth = params.containsKey("depth") ? (Integer)params.get("depth") : 1;
        if (depth < 0)
                throw new ParseException(ctx, "invalid negative stack depth");

        // Return action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                SwapCommand.this.checkDepth(session, depth * 2);
                final ArrayList<ArrayList<Channel<?>>> lists = new ArrayList<>(depth);
                for (int i = 0; i < 2; i++) {
                    final ArrayList<Channel<?>> list = new ArrayList<>(depth);
                    for (int j = 0; j < depth; j++)
                        list.add(SwapCommand.this.pop(session));
                    Collections.reverse(list);
                    lists.add(list);
                }
                for (ArrayList<Channel<?>> list : lists) {
                    for (Channel<?> channel : list)
                        SwapCommand.this.push(session, channel);
                }
            }
        };
    }
}

