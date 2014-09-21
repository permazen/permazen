
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JClass;
import org.jsimpledb.JTransaction;
import org.jsimpledb.parse.ObjTypeParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.IdentNode;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

@Function
public class AllFunction extends AbstractFunction {

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
    public Object parseParams(ParseSession session, ParseContext ctx, boolean complete) {

        // Verify parameter exists
        if (ctx.tryLiteral(")"))
            return null;

        // Special handling for tab-completion support for type names
        if (complete && (ctx.isEOF() || IdentNode.NAME_PATTERN.matcher(ctx.getInput()).matches())) {
            new ObjTypeParser().parse(session, ctx, complete);
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");
        }

        // Parse expression
        final int startingMark = ctx.getIndex();
        final Node node = ExprParser.INSTANCE.parse(session, ctx, complete);

        // If expression is a single identifier, re-parse it as an object type name, otherwise it's a class or int expression
        final Object result;
        if (node instanceof IdentNode) {
            ctx.setIndex(startingMark);
            result = new ObjTypeParser().parse(session, ctx, complete).getStorageId();
        } else
            result = node;

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return result;
    }

    @Override
    public Value apply(ParseSession session, Object param) {

        // Handle null
        if (param == null) {
            return new AbstractValue() {
                @Override
                public Object get(ParseSession session) {
                    return session.hasJSimpleDB() ?
                      JTransaction.getCurrent().getAll((Class<?>)null) : session.getTransaction().getAll();
                }
            };
        }

        // Handle storage ID
        if (param instanceof Integer)
            return this.getAll(session, (Integer)param);

        // Handle expression
        if (param instanceof Node) {
            final Object obj = ((Node)param).evaluate(session).checkNotNull(session, "all()");
            if (obj instanceof Number)
                return this.getAll(session, ((Number)obj).intValue());
            if (obj instanceof Class && session.hasJSimpleDB()) {
                return new AbstractValue() {
                    @Override
                    public Object get(ParseSession session) {
                        return JTransaction.getCurrent().getAll((Class<?>)obj);
                    }
                };
            }
            throw new EvalException("invalid object type expression with value of type " + obj.getClass().getName());
        }

        // Oops
        throw new RuntimeException("internal error");
    }

    private Value getAll(ParseSession session, final int storageId) {

        // Handle core-only case
        if (!session.hasJSimpleDB()) {
            return new AbstractValue() {
                @Override
                public Object get(ParseSession session) {
                    try {
                        return session.getTransaction().getAll(storageId);
                    } catch (IllegalArgumentException e) {
                        throw new EvalException(e.getMessage());
                    }
                }
            };
        }

        // Handle JSimpleDB case
        final JClass<?> jclass;
        try {
            jclass = JTransaction.getCurrent().getJSimpleDB().getJClass(storageId);
        } catch (IllegalArgumentException e) {
            throw new EvalException("no type with storage ID " + storageId + " exists in schema version "
              + JTransaction.getCurrent().getJSimpleDB().getLastVersion());
        }
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                return JTransaction.getCurrent().getAll(jclass);
            }
        };
    }
}

