
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseSession;

/**
 * {@link Node} representing a parsed {@link ParseSession} variable.
 *
 * @see ParseSession#getVars
 */
public class VarNode extends ConstNode {

    /**
     * Constructor.
     *
     * @param name variable name
     * @throws IllegalArgumentException if name is null
     * @throws IllegalArgumentException if name is not a valid Java identifier
     */
    public VarNode(String name) {
        super(new VarValue(name));
    }

    @Override
    public VarValue evaluate(ParseSession session) {
        return (VarValue)super.evaluate(session);
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return ((VarValue)super.evaluate(session)).getType(session);
    }

    /**
     * Get the variable name.
     *
     * @return variable name
     */
    public String getName() {
        return this.evaluate(null).getName();
    }
}

