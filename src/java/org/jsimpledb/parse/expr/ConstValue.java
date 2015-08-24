
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * A constant, read-only {@link Value}.
 */
public final class ConstValue extends AbstractValue {

    private final Object value;

    /**
     * Constructor.
     *
     * @param value constant result of evaluating this value
     */
    public ConstValue(Object value) {
        this.value = value;
    }

    @Override
    public Object get(ParseSession session) {
        return this.value;
    }

    @Override
    public String toString() {
        return "ConstValue[" + this.value + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ConstValue that = (ConstValue)obj;
        return this.value != null ? this.value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        return this.value != null ? this.value.hashCode() : null;
    }
}

