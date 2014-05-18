
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public class SpaceParser {

    private final boolean required;

    public SpaceParser() {
        this(false);
    }

    public SpaceParser(boolean required) {
        this.required = required;
    }

    public void parse(ParseContext ctx) {
        if (ctx.tryPattern("\\s+") == null) {
            if (required)
                throw new ParseException(ctx).addCompletion(" ");
        }
    }
}

