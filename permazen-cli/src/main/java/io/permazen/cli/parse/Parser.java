
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import io.permazen.cli.Session;

/**
 * Generic parsing interface.
 *
 * @param <T> parsed value type
 */
@FunctionalInterface
public interface Parser<T> {

    /**
     * Parse value from the given text.
     *
     * @param session CLI session
     * @param text input to parse
     * @return parsed value
     * @throws IllegalArgumentException if parse fails
     * @throws IllegalArgumentException if either parameter is null
     */
    T parse(Session session, String text);
}
