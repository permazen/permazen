
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.SpaceParser;

/**
 * Node representing a literal array instantiation expression, i.e., with curly braces initial values.
 */
public class LiteralArrayNode implements Node {

    private final Class<?> elemType;
    private final List<?> initialValue;

    /**
     * Constructor.
     *
     * @param elemType array component type
     * @param initialValue array initial value; each element in the list is either a {@link List} (all but the last dimension)
     *  or a {@code Node} (last dimension)
     */
    public LiteralArrayNode(Class<?> elemType, List<?> initialValue) {
        this.elemType = elemType;
        this.initialValue = initialValue;
    }

    @Override
    public Value evaluate(final ParseSession session) {
        return new ConstValue(LiteralArrayNode.createLiteral(session, this.elemType, this.initialValue));
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
     * @param elemType array component type
     * @return list of array elements, possibly nested
     */
    public static List<?> parseArrayLiteral(ParseSession session, ParseContext ctx, boolean complete, Class<?> elemType) {
        final SpaceParser spaceParser = new SpaceParser();
        final ArrayList<Object> list = new ArrayList<>();
        spaceParser.parse(ctx, complete);
        if (!ctx.tryLiteral("{"))
            throw new ParseException(ctx).addCompletion("{ ");
        spaceParser.parse(ctx, complete);
        if (ctx.tryLiteral("}"))
            return list;
        while (true) {
            list.add(elemType.isArray() ?
              LiteralArrayNode.parseArrayLiteral(session, ctx, complete, elemType.getComponentType()) :
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
