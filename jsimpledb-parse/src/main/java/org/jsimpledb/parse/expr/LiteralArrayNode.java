
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.util.ParseContext;

/**
 * Node representing a literal array instantiation expression, i.e., with curly braces initial values.
 */
public class LiteralArrayNode extends AbstractArrayNode {

    private final List<?> initialValue;

    /**
     * Constructor.
     *
     * @param baseTypeNode array base type class node
     * @param numDimensions number of array dimensions
     * @param initialValue array initial value; each element in the list is either a {@link List} (all but the last dimension)
     *  or a {@code Node} (last dimension)
     */
    public LiteralArrayNode(ClassNode baseTypeNode, int numDimensions, List<?> initialValue) {
        super(baseTypeNode, numDimensions);
        this.initialValue = initialValue;
    }

    @Override
    public Value evaluate(final ParseSession session) {
        final Class<?> elemType = ParseUtil.getArrayClass(this.getBaseType(session), this.numDimensions - 1);
        return new ConstValue(LiteralArrayNode.createLiteral(session, elemType, this.initialValue));
    }

    private static Object createLiteral(ParseSession session, Class<?> elemType, List<?> values) {
        final int length = values.size();
        final Object array = Array.newInstance(elemType, length);
        for (int i = 0; i < length; i++) {
            if (elemType.isArray())
                Array.set(array, i, LiteralArrayNode.createLiteral(session, elemType.getComponentType(), (List<?>)values.get(i)));
            else {
                try {
                    Array.set(array, i, ((Node)values.get(i)).evaluate(session).get(session));
                } catch (Exception e) {
                    throw new EvalException("error setting array value: " + e, e);
                }
            }
        }
        return array;
    }

    /**
     * Parse an array literal initial value expression.
     *
     * @param session parse session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @param dims number of array dimensions
     * @return list of array elements, possibly nested
     * @throws IllegalArgumentException if {@code dims} is less than one
     */
    public static List<?> parseArrayLiteral(ParseSession session, ParseContext ctx, boolean complete, int dims) {
        Preconditions.checkArgument(dims >= 1, "dims < 1");
        final SpaceParser spaceParser = new SpaceParser();
        final ArrayList<Object> list = new ArrayList<>();
        spaceParser.parse(ctx, complete);
        if (!ctx.tryLiteral("{"))
            throw new ParseException(ctx).addCompletion("{ ");
        spaceParser.parse(ctx, complete);
        if (ctx.tryLiteral("}"))
            return list;
        while (true) {
            list.add(dims > 1 ?
              LiteralArrayNode.parseArrayLiteral(session, ctx, complete, dims - 1) :
              ExprParser.INSTANCE.parse(session, ctx, complete));
            ctx.skipWhitespace();
            if (ctx.tryLiteral("}"))
                break;
            if (!ctx.tryLiteral(","))
                throw new ParseException(ctx, "expected `,'").addCompletion(", ");
            spaceParser.parse(ctx, complete);
        }
        return list;
    }
}
