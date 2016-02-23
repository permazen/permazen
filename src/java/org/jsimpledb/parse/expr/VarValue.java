
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import org.jsimpledb.JObject;
import org.jsimpledb.core.ObjId;
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
        Preconditions.checkArgument(value != null, "null value");

        // Add special hack to "refresh" JObjects into the session's transaction
        if (value instanceof ConstValue) {
            final Object obj = value.get(null);
            if (obj instanceof JObject && ((JObject)obj).getTransaction().equals(session.getJTransaction())) {
                final ObjId id = ((JObject)obj).getObjId();
                value = new AbstractValue() {
                    @Override
                    public Object get(ParseSession session) {
                        return session.getJTransaction().get(id);
                    }
                };
            }

        }

        // Save value
        session.getVars().put(this.name, value);
    }
}

