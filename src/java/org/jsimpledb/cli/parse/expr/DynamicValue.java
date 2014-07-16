
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import org.jsimpledb.cli.Session;

/**
 * Read/write values that are computed dynamically.
 */
public abstract class DynamicValue extends Value implements Setter {

    protected DynamicValue() {
        super(null);
    }

    @Override
    public abstract Object get(Session session);
}

