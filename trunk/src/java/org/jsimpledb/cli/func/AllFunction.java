
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import org.jsimpledb.JClass;
import org.jsimpledb.JTransaction;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ObjTypeParser;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.ExprParser;
import org.jsimpledb.cli.parse.expr.IdentNode;
import org.jsimpledb.cli.parse.expr.Node;
import org.jsimpledb.cli.parse.expr.Value;
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
        return "all([type])";
    }

    @Override
    public String getHelpDetail() {
        return "Retrieves all instances of the specified type, or all database objects if no type is given. The type may either"
          + " be the name of a JSimpleDB Java model type (as an identifier) or any expression which evaluates to a java.lang.Class"
          + " object (when not in core database mode) or an integer storage ID.";
    }

    @Override
    public Object parseParams(Session session, ParseContext ctx, boolean complete) {

        // Verify parameter exists
        if (ctx.tryLiteral(")"))
            return null;

        // Parse expression
        final int startingMark = ctx.getIndex();
        final Node node = ExprParser.INSTANCE.parse(session, ctx, complete);

        // If expression is a single identifier, re-parse it as an object type name, otherwise it's a class or int expression
        final Object result;
        if (node instanceof IdentNode) {
            ctx.setIndex(startingMark);
            result = new ObjTypeParser().parse(session, ctx, complete).getStorageId();
        } else if (session.hasJSimpleDB())          // must be java.lang.Class or integer expression - but only in non-core mode
            result = node;
        else
            throw new ParseException(ctx, "invalid object type expression");

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return result;
    }

    @Override
    public Value apply(Session session, Object param) {

        // Handle null
        if (param == null) {
            return new Value(session.hasJSimpleDB() ?
              JTransaction.getCurrent().getAll((Class<?>)null) : session.getTransaction().getAll());
        }

        // Handle storage ID
        if (param instanceof Integer)
            return this.getAll(session, (Integer)param);

        // Handle expression
        if (param instanceof Node) {
            final Object obj = ((Node)param).evaluate(session).checkNotNull(session, "all()");
            if (obj instanceof Class) {
                assert session.hasJSimpleDB();
                return new Value(JTransaction.getCurrent().getAll((Class<?>)obj));
            }
            if (obj instanceof Number)
                return this.getAll(session, ((Number)obj).intValue());
        }

        // Oops
        throw new RuntimeException("internal error");
    }

    private Value getAll(Session session, int storageId) {
        if (!session.hasJSimpleDB()) {
            try {
                return new Value(session.getTransaction().getAll(storageId));
            } catch (IllegalArgumentException e) {
                throw new EvalException(e.getMessage());
            }
        }
        final JClass<?> jclass;
        try {
            jclass = JTransaction.getCurrent().getJSimpleDB().getJClass(storageId);
        } catch (IllegalArgumentException e) {
            throw new EvalException("no type with storage ID " + storageId + " exists in schema version "
              + JTransaction.getCurrent().getJSimpleDB().getLastVersion());
        }
        return new Value(JTransaction.getCurrent().getAll(jclass));
    }
}

