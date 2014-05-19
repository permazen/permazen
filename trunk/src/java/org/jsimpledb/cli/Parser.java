
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

/**
 * Generic CLI parsing interface.
 */
public interface Parser<T> {

    /**
     * Parse text from the given parse context.
     *
     * @param session CLI session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @throws ParseException if parse fails, or if {@code complete} is true and there are valid completions
     */
    T parse(Session session, ParseContext ctx, boolean complete);
}

