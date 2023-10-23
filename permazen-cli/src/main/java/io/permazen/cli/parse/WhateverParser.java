
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import io.permazen.cli.Session;
import io.permazen.util.ParseContext;

/**
 * Accepts anything.
 */
public class WhateverParser implements Parser<String> {

    @Override
    public String parse(Session session, ParseContext ctx, boolean complete) {
        final String result = ctx.getInput();
        ctx.setIndex(ctx.getOriginalInput().length());
        return result;
    }
}
