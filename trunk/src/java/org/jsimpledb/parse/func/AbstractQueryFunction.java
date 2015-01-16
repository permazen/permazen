
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.core.ObjType;
import org.jsimpledb.parse.ObjTypeParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.LiteralNode;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

/**
 * Support superclass for index queries (simple or composite). Function parameters take one of these forms:
 * <ol>
 * <li>{@code function(object-type, name, value-type, ...)} (JSimpleDB mode only)\n"
 * <li>{@code function(name)}</li>
 * <li>{@code function(storage-id)}</li>
 * </ol>
 */
abstract class AbstractQueryFunction extends AbstractFunction {

    private final int minValueTypes;
    private final int maxValueTypes;

    protected AbstractQueryFunction(String name, int minValueTypes, int maxValueTypes) {
        super(name);
        if (minValueTypes < 1 || maxValueTypes < minValueTypes)
            throw new IllegalArgumentException("invalid min/max");
        this.minValueTypes = minValueTypes;
        this.maxValueTypes = maxValueTypes;
    }

    /**
     * Returns one of: {@link Integer}, {@link Node}, or {@link Node}{@code []}.
     */
    @Override
    public Object parseParams(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Check existence of first parameter
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "at least one parameter required");

        // Attempt to parse second form
        final int typeStart = ctx.getIndex();
        int storageId = -1;
        try {
            storageId = this.parseName(session, ctx, complete);
        } catch (ParseException e) {
            if (complete && !e.getCompletions().isEmpty())
                throw e;
        }
        if (storageId != -1) {
            ctx.skipWhitespace();
            if (!ctx.tryLiteral(")"))
                throw new ParseException(ctx, "expected `)'").addCompletion(") ");
            return storageId;
        }
        ctx.setIndex(typeStart);

        // Attempt to parse first form with type name (JSimpleDB mode only)
        Node param1 = null;
        if (session.hasJSimpleDB()) {
            try {
                final ObjType objType = new ObjTypeParser().parse(session, ctx, complete);
                final int mark = ctx.getIndex();
                ctx.skipWhitespace();
                if (!ctx.tryLiteral(","))                   // verify this is the first form
                    throw new ParseException(ctx);
                ctx.setIndex(mark);
                param1 = new LiteralNode(session.getJSimpleDB().getJClass(objType.getStorageId()).getType());
            } catch (ParseException e) {
                ctx.setIndex(typeStart);
            }
        }

        // Parse first parameter as expression
        if (param1 == null)
            param1 = ExprParser.INSTANCE.parse(session, ctx, complete);

        // Only one parameter? Assume it's the third form.
        ctx.skipWhitespace();
        if (ctx.tryLiteral(")"))
            return param1;

        // Multi-parameter form requires JSimpleDB mode
        if (!session.hasJSimpleDB())
            throw new ParseException(ctx, "expected `)' (JSimpleDB mode required for multiple params)").addCompletion(") ");
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,' between " + this.getName() + "() function parameters").addCompletion(", ");
        this.spaceParser.parse(ctx, complete);

        // Parse value types
        final Node[] valueTypeNodes = this.parseExpressionParams(session,
          ctx, complete, 1, 1 + this.minValueTypes, 1 + this.maxValueTypes);

        // Done
        final Node[] result = new Node[1 + valueTypeNodes.length];
        result[0] = param1;
        System.arraycopy(valueTypeNodes, 0, result, 1, valueTypeNodes.length);
        return result;
    }

    /**
     * Attempt to parse an index name.
     *
     * @return indexed field or composite index storage ID
     */
    protected abstract int parseName(ParseSession session, ParseContext ctx, boolean complete);

    @Override
    public Value apply(ParseSession session, Object result) {

        // Handle second and third forms
        Integer storageId = null;
        if (result instanceof Node)
            storageId = (((Node)result).evaluate(session)).checkIntegral(session, "queryIndex()");
        else if (result instanceof Integer)
            storageId = (Integer)result;
        if (storageId != null)
            return this.apply(session, (int)storageId);

        // Handle first form
        final Node[] params = (Node[])result;
        final Class<?> objectType = params[0].evaluate(session).checkType(session, this.getName(), Class.class);
        final String fieldName = params[1].evaluate(session).checkType(session, this.getName(), String.class);
        final Class<?>[] valueTypes = new Class<?>[params.length - 2];
        for (int i = 0; i < valueTypes.length; i++)
            valueTypes[i] = params[2 + i].evaluate(session).checkType(session, this.getName(), Class.class);
        return this.apply(session, objectType, fieldName, valueTypes);
    }

    /**
     * Handle single argument form.
     *
     * @param session parse session
     * @param storageId field or composite index storage ID
     */
    protected abstract Value apply(ParseSession session, int storageId);

    /**
     * Handle multi-argument form.
     *
     * @param session parse session
     * @param objectType target object type
     * @param name field or composite index name
     * @param valueTypes index value type(s)
     */
    protected abstract Value apply(ParseSession session, Class<?> objectType, String name, Class<?>[] valueTypes);
}

