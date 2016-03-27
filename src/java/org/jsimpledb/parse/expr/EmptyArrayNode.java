
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.func.AbstractFunction;

/**
 * Node representing an "empty" array instantiation expression, i.e., with dimensions but no literal values.
 */
public class EmptyArrayNode implements Node {

    private final Class<?> elemType;
    private final List<Node> dimensionList;

    /**
     * Constructor.
     */
    public EmptyArrayNode(Class<?> elemType, List<Node> dimensionList) {
        this.elemType = elemType;
        this.dimensionList = dimensionList;
    }

    @Override
    public Value evaluate(final ParseSession session) {
        return new ConstValue(EmptyArrayNode.createEmpty(session, this.elemType, this.dimensionList));
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
