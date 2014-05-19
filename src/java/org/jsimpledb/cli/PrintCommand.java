
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.util.ParseContext;

public class PrintCommand extends Command {

    public PrintCommand() {
        super("print -v:verbose -n:noRemove depth:int?");
    }

    @Override
    public String getHelpSummary() {
        return "prints data items in channel";
    }

    @Override
    public String getHelpDetail() {
        return "The `print' command prints the contents of the specified channel to the console and removes it from the stack."
          + " A stack depth may be specified; the default is zero (top of stack). If the `-n' flag is given, the channel"
          + " is not removed from the stack.";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final boolean verbose = params.containsKey("verbose");
        final boolean noRemove = params.containsKey("noRemove");
        final int depth = params.containsKey("depth") ? (Integer)params.get("depth") : 0;
        if (depth < 0)
            throw new ParseException(ctx, "invalid negative depth");

        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final Channel<?> channel = noRemove ?
                  PrintCommand.this.get(session, depth) : PrintCommand.this.remove(session, depth);
                PrintCommand.this.print(session, channel, verbose);
            }
        };
    }

    private <T> void print(Session session, Channel<T> channel, boolean verbose) {
        final ItemType<T> itemType = channel.getItemType();
        for (T value : channel.getItems(session))
            itemType.print(session, value, verbose);
    }
}

