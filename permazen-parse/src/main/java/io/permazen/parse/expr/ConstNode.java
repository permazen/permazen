
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;

import io.permazen.parse.ParseSession;

/**
 * A {@link Node} that always evaluates to the same {@link Value}.
 */
public class ConstNode implements Node {

    private final Value value;

    public ConstNode(Value value) {
        Preconditions.checkArgument(value != null, "null value");
        this.value = value;
    }

    @Override
    public Value evaluate(ParseSession session) {
        return this.value;
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return this.value.getType(session);
    }
}

