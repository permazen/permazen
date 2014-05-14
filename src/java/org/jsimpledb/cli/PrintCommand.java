
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class PrintCommand extends AbstractCommand {

    public PrintCommand() {
        super("print");
    }

    @Override
    public String getUsage() {
        return this.name + " [-v] [-n] [depth]";
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
    public Action parseParameters(Session session, ParseContext ctx) {
        final ParamParser parser = new ParamParser(0, 1, this.getUsage(), "-v", "-n").parse(ctx);
        final boolean verbose = parser.hasFlag("-v");
        final boolean noRemove = parser.hasFlag("-n");
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
            depth = 0;
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

