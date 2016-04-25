
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import org.jsimpledb.parse.ParseSession;

/**
 * A {@link Value} that holds a reference to some other {@link Value}.
 */
public class ValueValue extends AbstractLValue {

    private Value value;

    /**
     * Constructor.
     *
     * <p>
     * This instance will have no initial value; until one is set, {@link #get get()} will throw an exception.
     */
    public ValueValue() {
        this(null);
    }

    /**
     * Constructor.
     *
     * @param value initial value, or null for none
     */
    public ValueValue(Value value) {
        this.value = value;
    }

    @Override
    public Object get(ParseSession session) {
        if (this.value == null)
            throw new EvalException("value not defined");
        return this.value.get(session);
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return this.value.getType(session);
    }

    @Override
    public void set(ParseSession session, Value value) {
        Preconditions.checkArgument(value != null, "null value");
        this.value = value;
    }
}

