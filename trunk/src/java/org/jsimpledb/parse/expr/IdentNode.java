
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * Identifier.
 */
public class IdentNode extends AbstractNamed implements Node {

    /**
     * Constructor.
     *
     * @param name variable name
     * @throws IllegalArgumentException if name is null
     * @throws IllegalArgumentException if name is not a valid Java identifier
     */
    public IdentNode(String name) {
        super(name);
    }

// Node

    @Override
    public Value evaluate(ParseSession session) {
        throw new RuntimeException("internal error: can't evaluate ident `" + this.name + "'");
    }
}

