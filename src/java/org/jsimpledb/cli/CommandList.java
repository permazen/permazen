
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class CommandList implements Parser<Channels> {

    @Override
    public Channels parse(Session session, Channels input, ParseContext ctx) {
        final Channels output = new Command().parse(session, input, ctx);
        ctx.skipWhitespace();
        if (ctx.tryLiteral(","))
            output.addAll(new CommandList().parse(session, input, ctx));
        return output;
    }
}

