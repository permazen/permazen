
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ObjTypeParser;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

@CliFunction
public class AllFunction extends Function {

    private final SpaceParser spaceParser = new SpaceParser();

    public AllFunction() {
        super("all");
    }

    @Override
    public String getHelpSummary() {
        return "get all objects of a specified type";
    }

    @Override
    public String getUsage() {
        return "all(type)";
    }

    @Override
    public String getHelpDetail() {
        return "Retrieves all instances of the specified type.";
    }

    @Override
    public Integer parseParams(Session session, ParseContext ctx, boolean complete) {

        // Get object type
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "type parameter required");
        final int storageId = new ObjTypeParser().parse(session, ctx, complete).getStorageId();

        // Finish parse
        this.spaceParser.parse(ctx, complete);
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return storageId;
    }

    @Override
    public Value apply(Session session, Object params) {
        final int storageId = (Integer)params;
        final Transaction tx = session.getTransaction();
        return new Value(tx.getAll(storageId));
    }
}

