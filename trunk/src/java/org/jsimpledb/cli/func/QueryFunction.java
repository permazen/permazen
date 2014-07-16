
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import org.jsimpledb.JTransaction;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.IndexedFieldParser;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.util.ParseContext;

@CliFunction
public class QueryFunction extends Function {

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
    public Integer parseParams(final Session session, final ParseContext ctx, final boolean complete) {

        // Parse indexed field parameter
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "indexed field parameter required");
        final int storageId = new IndexedFieldParser().parse(session, ctx, complete);

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return storageId;
    }

    @Override
    public Value apply(Session session, Object params) {
        final int storageId = (Integer)params;
        return new Value(session.hasJSimpleDB() ?
          JTransaction.getCurrent().querySimpleField(storageId) : session.getTransaction().querySimpleField(storageId));
    }
}

