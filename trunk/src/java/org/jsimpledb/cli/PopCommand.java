
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class PopCommand extends Command {

    public PopCommand() {
        super("pop");
    }

    @Override
    public String getUsage() {
        return this.name + " [depth]";
    }

    @Override
    public String getHelpSummary() {
        return "pops one or more channels off the top of the channel stack";
    }

    @Override
    public String getHelpDetail() {
        return "The `pop' command pops one or more channels off the top of the channel stack. By default only the top"
          + " channel is popped; specify a different depth to pop more than one channel.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {

        // Parse parameters
        final Map<String, Object> params = new ParamParser(this, "depth:int?").parseParameters(session, ctx, complete);
        final int depth = params.containsKey("depth") ? (Integer)params.get("depth") : 1;
        if (depth < 0)
            throw new ParseException(ctx, "invalid negative depth");

        // Return action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                PopCommand.this.checkDepth(session, depth);
                for (int i = 0; i < depth; i++)
                    PopCommand.this.pop(session);
            }
        };
    }
}

