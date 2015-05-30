
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * A {@link Node} that always evaluates to the same {@link Value}.
 */
public class ConstNode implements Node {

    private final Value value;

    public ConstNode(Value value) {
        if (value == null)
            throw new IllegalArgumentException("null value");
        this.value = value;
    }

    @Override
    public Value evaluate(ParseSession session) {
        return this.value;
    }
}

