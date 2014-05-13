
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class Pipeline implements Parser<Channels> {

    @Override
    public Channels parse(Session session, Channels input, ParseContext ctx) {
        ctx.skipWhitespace();
        if (ctx.tryLiteral("(")) {
            Channels output = new Pipeline().parse(session, input, ctx);
            ctx.expect(')');
            return output;
        }
        Channels output = new CommandList().parse(session, input, ctx);
        ctx.skipWhitespace();
        if (ctx.tryLiteral("|"))
            output = new Pipeline().parse(session, output, ctx);
        return output;
    }
}

