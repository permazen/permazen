
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class PopCommand extends AbstractCommand {

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
                PopCommand.this.checkDepth(session, depth);
                for (int i = 0; i < depth; i++)
                    PopCommand.this.pop(session);
            }
        };
    }
}

