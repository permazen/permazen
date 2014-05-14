
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.Collections;

import org.jsimpledb.util.ParseContext;

public class SwapCommand extends AbstractCommand {

    public SwapCommand() {
        super("swap");
    }

    @Override
    public String getUsage() {
        return this.name + " [depth]";
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
    public Action parseParameters(Session session, ParseContext ctx) {

        // Parse parameters
        final ParamParser parser = new ParamParser(0, 1, this.getUsage()).parse(ctx);
        final int depth;
        if (parser.getParams().size() > 0) {
            final String depthParam = parser.getParam(0);
            try {
                depth = Integer.parseInt(depthParam);
                if (depth < 0)
                    throw new IllegalArgumentException("depth is negative");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid depth `" + depthParam + "'");
            }
        } else
            depth = 1;

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

