
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Value} that represents a {@link ParseSession} variable.
 *
 * @see ParseSession#getVars
 */
public class VarValue extends AbstractLValue {

    private final String name;

    /**
     * Constructor.
     *
     * @param name variable name
     * @throws IllegalArgumentException if name is null
     * @throws IllegalArgumentException if name is not a valid Java identifier
     */
    public VarValue(String name) {
        new AbstractNamed(name) { };                                // validates the name
        this.name = name;
    }

    /**
     * Get the variable name.
     *
     * @return variable name
     */
    public String getName() {
        return this.name;
    }

    @Override
    public Object get(ParseSession session) {
        final Value value = session.getVars().get(this.name);
        if (value == null)
            throw new EvalException("variable `" + name + "' is not defined");
        return value.get(session);
    }

    @Override
    public void set(ParseSession session, Value value) {
        if (value == null)
            throw new IllegalArgumentException("null value");
        session.getVars().put(this.name, value);
    }
}

