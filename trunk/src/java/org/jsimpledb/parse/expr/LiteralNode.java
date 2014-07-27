
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * A parsed literal value.
 */
public class LiteralNode implements Node {

    private final Object value;

    public LiteralNode(Object value) {
        this.value = value;
    }

    @Override
    public Value evaluate(ParseSession session) {
        return new Value(this.value);
    }

    @Override
    public String toString() {
        return "Literal[" + this.value + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final LiteralNode that = (LiteralNode)obj;
        return this.value != null ? this.value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        return this.value != null ? this.value.hashCode() : null;
    }
}

