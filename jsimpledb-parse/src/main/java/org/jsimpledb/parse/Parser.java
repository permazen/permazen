
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse;

import org.jsimpledb.util.ParseContext;

/**
 * Generic parsing interface.
 *
 * @param <T> parsed value type
 */
@FunctionalInterface
public interface Parser<T> {

    /**
     * Parse text from the given parse context.
     *
     * <p>
     * Generally speaking, this method may assume that any whitespace allowed before the item
     * being parsed has already been skipped over (that's a matter for the containing parser).
     *
     * @param session parse session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @return parsed value
     * @throws ParseException if parse fails, or if {@code complete} is true and there are valid completions
     */
    T parse(ParseSession session, ParseContext ctx, boolean complete);
}

