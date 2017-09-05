
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseSession;
import io.permazen.parse.ParseUtil;

/**
 * Superclass for array instantiation nodes.
 */
abstract class AbstractArrayNode implements Node {

    protected final ClassNode baseTypeNode;
    protected final int numDimensions;

    protected AbstractArrayNode(ClassNode baseTypeNode, int numDimensions) {
        this.baseTypeNode = baseTypeNode;
        this.numDimensions = numDimensions;
    }

    /**
     * Resolve the array base type.
     *
     * @param session parse session
     * @return resolved array base type
     * @throws EvalException if base type is {@code void}
     * @throws EvalException if base type cannot be resolved
     */
    protected Class<?> getBaseType(ParseSession session) {
        final Class<?> baseType = this.baseTypeNode.resolveClass(session);
        if (baseType == void.class)
            throw new EvalException("illegal instantiation of void array");
        return baseType;
    }

    @Override
    public Class<?> getType(ParseSession session) {
        try {
            return ParseUtil.getArrayClass(this.getBaseType(session), this.numDimensions);
        } catch (EvalException e) {
            return Object.class;
        }
    }
}
