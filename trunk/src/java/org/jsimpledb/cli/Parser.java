
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.util.ParseContext;

public interface Parser<T> {

    /**
     * Parse as much text as possible, updating the given parse context, and return the result.
     *
     * @throws ParseException if no prefix of the input is valid
     */
    T parse(Session session, Channels channels, ParseContext ctx);
}

