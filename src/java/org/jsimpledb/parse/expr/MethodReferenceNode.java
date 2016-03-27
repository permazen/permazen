
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

/**
 * {@link Node} representing a method reference.
 */
public abstract class MethodReferenceNode extends TypeInferringNode {

    final String name;

    /**
     * Constructor.
     *
     * @param name method name
     * @throws IllegalArgumentException if {@code name} is null
     */
    protected MethodReferenceNode(String name) {
        Preconditions.checkArgument(name != null, "null name");
        this.name = name;
    }
}
