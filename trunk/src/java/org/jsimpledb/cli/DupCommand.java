
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.Collections;

import org.jsimpledb.util.ParseContext;

public class DupCommand extends AbstractCommand {

    public DupCommand() {
        super("dup");
    }

    @Override
    public String getUsage() {
        return this.name + " [depth]";
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

