
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ComplexField;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.parse.IndexedFieldParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.Value;

@Function
public class QueryFunction extends AbstractFunction {

    private final SpaceParser spaceParser = new SpaceParser();

    public QueryFunction() {
        super("query");
    }

    @Override
    public String getHelpSummary() {
        return "queries an indexed field";
    }

    @Override
    public String getUsage() {
        return "query(type.field)";
    }

    @Override
    public String getHelpDetail() {
        return "Queries a field index and returns a mapping from field value to set of objects containing that value in the field."
          + " For collection fields, specify the sub-field, e.g., `Person.friends.element' or `Person.grades.key'."
          + " Indexes on list elements and map values also include corresponding list indicies or map keys (respectively).";
    }

    @Override
    public IndexedFieldParser.Result parseParams(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Parse indexed field parameter
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "indexed field parameter required");
        final IndexedFieldParser.Result result = new IndexedFieldParser().parse(session, ctx, complete);

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return result;
    }

    @Override
    public Value apply(ParseSession session, Object params) {
        final IndexedFieldParser.Result result = (IndexedFieldParser.Result)params;
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                return session.hasJSimpleDB() ?
                  QueryFunction.this.query(JTransaction.getCurrent(), result) :
                  QueryFunction.this.query(session.getTransaction(), result);
            }
        };
    }

    private Object query(JTransaction jtx, IndexedFieldParser.Result result) {
        return jtx.queryIndex(result.getField().getStorageId());
    }

    private Object query(Transaction tx, IndexedFieldParser.Result result) {
        final SimpleField<?> field = result.getField();
        final ComplexField<?> parentField = result.getParentField();
        if (parentField instanceof ListField<?>)
            return tx.queryListField(parentField.getStorageId());
        else if (parentField instanceof MapField<?, ?> && ((MapField<?, ?>)parentField).getValueField().equals(field))
            return tx.queryMapValueField(parentField.getStorageId());
        else
            return tx.querySimpleField(field.getStorageId());
    }
}

