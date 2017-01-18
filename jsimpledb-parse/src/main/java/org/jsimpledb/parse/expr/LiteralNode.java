
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.util.Objects;

import org.jsimpledb.parse.ParseSession;

/**
 * A parsed literal value. Evaluates to a {@link ConstValue}.
 */
public class LiteralNode implements Node {

    private final Object value;

    /**
     * Constructor.
     *
     * @param value the literal value
     */
    public LiteralNode(Object value) {
        this.value = value;
    }

    /**
     * Get the literal value.
     *
     * @return literal value
     */
    public Object getLiteralValue() {
        return this.value;
    }

    @Override
    public ConstValue evaluate(ParseSession session) {
        return new ConstValue(this.value);
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return this.value != null ? this.value.getClass() : Object.class;
    }

    @Override
    public String toString() {
        return "LiteralNode[" + this.value + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final LiteralNode that = (LiteralNode)obj;
        return Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.value);
    }
}

