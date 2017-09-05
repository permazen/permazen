
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse;

import io.permazen.util.ParseContext;

/**
 * Parses whitespace.
 */
public class SpaceParser {

    private final boolean required;
    private final boolean weak;

    public SpaceParser() {
        this(false);
    }

    public SpaceParser(boolean required) {
        this(required, /*false*/true);
    }

    public SpaceParser(boolean required, boolean weak) {
        this.required = required;
        this.weak = weak;
    }

    public void parse(ParseContext ctx, boolean complete) {
        if (ctx.tryPattern("\\s+") != null)
            return;
        if (required)
            throw new ParseException(ctx).addCompletion(" ");
        if (!complete)
            return;
        if (ctx.getIndex() != 0 && Character.isWhitespace(ctx.getOriginalInput().charAt(ctx.getIndex() - 1)))
            return;
        if (this.weak && !ctx.isEOF())
            return;
        throw new ParseException(ctx).addCompletion(" ");
    }
}

