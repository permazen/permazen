
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

/**
 * Runtime value representing a session variable.
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

    /**
     * Get the variable name.
     */
    public String getName() {
        return ((VarValue)this.evaluate(null)).getName();
    }
}

