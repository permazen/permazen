
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * Provided by {@link Value}s that are capable of assignment.
 */
public interface Setter {

    /**
     * Make assignment.
     *
     * @param value new value
     * @throws IllegalArgumentException if {@code value} is null or otherwise invalid
     */
    void set(ParseSession session, Value value);
}

