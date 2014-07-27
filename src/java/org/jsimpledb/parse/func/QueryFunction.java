
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
import org.jsimpledb.parse.SpaceParser;
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
          + " For collection fields, specify the sub-field, e.g., `Person.friends.element' or `Person.grades.key'.";
    }

    @Override
    public Integer parseParams(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Parse indexed field parameter
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "indexed field parameter required");
        final int storageId = new IndexedFieldParser().parse(session, ctx, complete).getField().getStorageId();

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return storageId;
    }

    @Override
    public Value apply(ParseSession session, Object params) {
        final int storageId = (Integer)params;
        return new Value(null) {
            @Override
            public Object get(ParseSession session) {
                return session.hasJSimpleDB() ?
                  JTransaction.getCurrent().querySimpleField(storageId) : session.getTransaction().querySimpleField(storageId);
            }
        };
    }
}

