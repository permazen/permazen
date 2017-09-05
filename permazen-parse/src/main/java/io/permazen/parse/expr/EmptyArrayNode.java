
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseSession;
import io.permazen.parse.ParseUtil;

import java.lang.reflect.Array;
import java.util.List;

/**
 * Node representing an "empty" array instantiation expression, i.e., with dimensions but no literal values.
 */
public class EmptyArrayNode extends AbstractArrayNode {

    private final List<Node> dimensionList;

    /**
     * Constructor.
     *
     * @param baseTypeNode array base type class node
     * @param dimensionList list containing the sizes of each array dimension
     */
    public EmptyArrayNode(ClassNode baseTypeNode, List<Node> dimensionList) {
        super(baseTypeNode, dimensionList.size());
        this.dimensionList = dimensionList;
    }

    @Override
    public Value evaluate(final ParseSession session) {
        final Class<?> elemType = ParseUtil.getArrayClass(this.getBaseType(session), this.numDimensions - 1);
        return new ConstValue(EmptyArrayNode.createEmpty(session, elemType, this.dimensionList));
    }

    private static Object createEmpty(ParseSession session, Class<?> elemType, List<Node> dims) {
        final int length = dims.get(0).evaluate(session).checkIntegral(session, "array creation");
        final Object array = Array.newInstance(elemType, length);
        final List<Node> remainingDims = dims.subList(1, dims.size());
        if (!remainingDims.isEmpty() && remainingDims.get(0) != null) {
            for (int i = 0; i < length; i++)
                Array.set(array, i, EmptyArrayNode.createEmpty(session, elemType.getComponentType(), remainingDims));
        }
        return array;
    }
}
