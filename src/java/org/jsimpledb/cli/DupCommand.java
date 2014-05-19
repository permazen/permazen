
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

public class DupCommand extends Command {

    public DupCommand() {
        super("dup depth:int?");
    }

    @Override
    public String getHelpSummary() {
        return "duplicates channel(s) on top of the channel stack";
    }

    @Override
    public String getHelpDetail() {
        return "The `dup' command duplicates the top channel and pushes the copy onto the channel stack. If a `depth'"
         + " other than `1' (the default) is given, that many channels are duplicated and pushed (the copies will appear"
         + " in the same order as the originals).";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final int depth = params.containsKey("depth") ? (Integer)params.get("depth") : 1;
        if (depth < 0)
            throw new ParseException(ctx, "invalid negative depth");

        // Return action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                DupCommand.this.checkDepth(session, depth);
                final ArrayList<Channel<?>> channels = new ArrayList<>(depth);
                for (int i = 0; i < depth; i++)
                    channels.add(DupCommand.this.get(session, i));
                Collections.reverse(channels);
                for (Channel<?> channel : channels)
                    DupCommand.this.push(session, channel);
            }
        };
    }
}

