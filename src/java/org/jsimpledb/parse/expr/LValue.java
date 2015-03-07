
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * Extension of the {@link Value} interface for instances that are capable of assignment.
 */
public interface LValue extends Value {

    /**
     * Make assignment to this instance.
     *
     * @param session parse session
     * @param value new value for this instance
     * @throws IllegalArgumentException if {@code value} is null or otherwise invalid
     */
    void set(ParseSession session, Value value);
}

