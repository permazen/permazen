
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import org.jsimpledb.cli.Session;

/**
 * Provided by {@link Value}s that are capable of assignment.
 */
public interface Setter {

    /**
     * Make assignment.
     *
     * @param value new value
     * @throws IllegalArgumentException if {@code value} is invalid
     */
    void set(Session session, Object value);
}

