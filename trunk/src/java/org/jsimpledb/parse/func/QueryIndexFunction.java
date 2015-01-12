
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JTransaction;
import org.jsimpledb.parse.IndexedFieldParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

@Function
public class QueryIndexFunction extends AbstractFunction {

    public QueryIndexFunction() {
        super("queryIndex");
    }

    @Override
    public String getHelpSummary() {
        return "queries the index associated with an indexed field";
    }

    @Override
    public String getUsage() {
        return "queryIndex(object-type, field-name, value-type) (JSimpleDB mode only)\n"
          + "       queryIndex(type-name.field-name)"
          + "       queryIndex(storage-id)";
    }

    @Override
    public String getHelpDetail() {
        return "Queries the simple index associated with an indexed field. The object-type is the type of object to be"
          + " queried, i.e., the object type that contains the indexed field, or any super-type or sub-type; a strict"
          + " sub-type will cause the returned index to be restricted to that sub-type. The field-name"
          + " is the name of the field to query; for collection fields, this must include the sub-field name, e.g.,"
          + " `mylist.element' or `mymap.value'. The value-type is the field's value type; in the case of reference fields,"
          + " a more restrictive sub-type may also be specified, otherwise the field type must exactly match the field."
          + "\n\nThe first form is only valid in JSimpleDB mode; the second and third forms may be used in either JSimpleDB"
          + " mode or Core API mode.";
    }

    @Override
    public Object parseParams(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Check existence of first parameter
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "at least one parameter required");

        // Attempt to parse second form
        final int typeStart = ctx.getIndex();
        IndexedFieldParser.Result fieldResult;
        try {
            fieldResult = new IndexedFieldParser().parse(session, ctx, complete);
        } catch (ParseException e) {
            if (complete && !e.getCompletions().isEmpty())
                throw e;
            fieldResult = null;
        }
        if (fieldResult != null) {

            // Finish parse
            ctx.skipWhitespace();
            if (!ctx.tryLiteral(")"))
                throw new ParseException(ctx, "expected `)'").addCompletion(") ");

            // Done
            return fieldResult.getField().getStorageId();
        }
        ctx.setIndex(typeStart);

        // Parse first parameter
        final Node param1 = ExprParser.INSTANCE.parse(session, ctx, complete);

        // Only one parameter?
        ctx.skipWhitespace();
        if (ctx.tryLiteral(")"))
            return param1;

        // First form requires JSimpleDB mode
        if (!session.hasJSimpleDB())
            throw new ParseException(ctx, "expected `)' (JSimpleDB mode required for multiple params)").addCompletion(") ");

        // Parse second and third parameters (JSimpleDB mode only)
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,' between " + this.name + "() function parameters").addCompletion(", ");
        this.spaceParser.parse(ctx, complete);
        final Node param2 = ExprParser.INSTANCE.parse(session, ctx, complete);
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,' between " + this.name + "() function parameters").addCompletion(", ");
        this.spaceParser.parse(ctx, complete);
        final Node param3 = ExprParser.INSTANCE.parse(session, ctx, complete);
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return new Node[] { param1, param2, param3 };
    }

    @Override
    public Value apply(ParseSession session, Object result) {

        // Handle second and third forms
        final Integer storageId;
        if (result instanceof Node)
            storageId = (((Node)result).evaluate(session)).checkIntegral(session, "queryIndex()");
        else if (result instanceof Integer)
            storageId = (Integer)result;
        else
            storageId = null;
        if (storageId != null) {
            return new AbstractValue() {
                @Override
                public Object get(ParseSession session) {
                    return session.getTransaction().queryIndex(storageId);
                }
            };
        }

        // Handle first form
        final Node[] params = (Node[])result;
        final Class<?> objectType = params[0].evaluate(session).checkType(session, "queryIndex()", Class.class);
        final String fieldName = params[1].evaluate(session).checkType(session, "queryIndex()", String.class);
        final Class<?> valueType = params[2].evaluate(session).checkType(session, "queryIndex()", Class.class);
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                return JTransaction.getCurrent().queryIndex(objectType, fieldName, valueType);
            }
        };
    }
}

