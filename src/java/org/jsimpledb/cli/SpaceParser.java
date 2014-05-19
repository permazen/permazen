
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

/**
 * Parses whitespace.
 */
public class SpaceParser {

    private final boolean required;

    public SpaceParser() {
        this(false);
    }

    public SpaceParser(boolean required) {
        this.required = required;
    }

    public void parse(ParseContext ctx, boolean complete) {
        if (ctx.tryPattern("\\s+") == null) {
            if (required
              || (complete && (ctx.getIndex() == 0 || !Character.isWhitespace(ctx.getOriginalInput().charAt(ctx.getIndex() - 1)))))
                throw new ParseException(ctx, "missing parameters").addCompletion(" ");
        }
    }
}

