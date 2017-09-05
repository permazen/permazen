
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseSession;

import java.util.Objects;

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
    public Class<?> getType(ParseSession session) {
        return this.value != null ? this.value.getClass() : Object.class;
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
        return Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.value);
    }
}

