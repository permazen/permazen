
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import com.google.common.collect.Iterables;

import java.util.regex.Matcher;

import org.jsimpledb.JClass;
import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.IdentNode;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.util.ParseCastFunction;

@Function(worksInCoreAPIMode = false)
public class InvertFunction extends AbstractFunction {

    public InvertFunction() {
        super("invert");
    }

    @Override
    public String getHelpSummary() {
        return "invert a path of references";
    }

    @Override
    public String getUsage() {
        return "invert(type.fields..., objects)";
    }

    @Override
    public String getHelpDetail() {
        return "The invert() function inverts a reference path. The first parameter is the reference path, in the form"
          + " of type name followed by a dot-separated list of one or more reference fields. The second parameter is a"
          + " collection of target objects. The return value is the set of all objects that refer to any of the target"
          + " objects through the path of references. This function does not work when running in core API mode.";
    }

    @Override
    public InvertInfo parseParams(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Sanity check
        if (!session.hasJSimpleDB())
            throw new ParseException(ctx, "invert() does not work in core API mode");

        // Parse entire reference path
        final int mark = ctx.getIndex();
        final Matcher matcher = ctx.tryPattern(IdentNode.NAME_PATTERN + "\\s*(\\.\\s*" + IdentNode.NAME_PATTERN + "\\s*)*");
        if (matcher == null)
            throw new ParseException(ctx, "invalid reference path");
        final String path = matcher.group().replaceAll("\\s+", "");
        final int firstDot = path.indexOf('.');
        if (firstDot == -1) {
            ctx.setIndex(mark);
            throw new ParseException(ctx, "invalid reference path");
        }
        final String typeName = path.substring(0, firstDot);
        final String fieldPath = path.substring(firstDot + 1);

        // Find start type
        final JClass<?> jclass;
        try {
            jclass = session.getJSimpleDB().getJClass(session.getNameIndex().getSchemaObjectType(typeName).getStorageId());
        } catch (UnknownTypeException e) {
            ctx.setIndex(mark);
            throw new ParseException(ctx, "invalid type `" + typeName + "': " + e.getMessage(), e);
        }

        // Verify reference path is valid (although we don't verify the last field in the path is a reference field here)
        try {
            session.getJSimpleDB().parseReferencePath(jclass.getType(), fieldPath);
        } catch (IllegalArgumentException e) {
            ctx.setIndex(mark);
            throw new ParseException(ctx, "invalid path `" + fieldPath + "': " + e.getMessage(), e);
        }

        // Parase expression
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,'").addCompletion(", ");
        this.spaceParser.parse(ctx, complete);
        final Node expr = new ExprParser().parse(session, ctx, complete);

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return new InvertInfo(jclass, fieldPath, expr);
    }

    @Override
    public Value apply(ParseSession session, Object params) {
        final InvertInfo info = (InvertInfo)params;
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {

                // Evaluate expression
                final Iterable<?> items = info.getExpr().evaluate(session).checkType(session, "invert()", Iterable.class);
                final Iterable<JObject> jobjs = Iterables.transform(items, new ParseCastFunction<JObject>(JObject.class));

                // Invert references to iterated objects
                try {
                    return JTransaction.getCurrent().invertReferencePath(info.getJClass().getType(), info.getFieldPath(), jobjs);
                } catch (Exception e) {
                    throw new EvalException("invert() failed: " + e, e);
                }
            }
        };
    }

// InvertInfo

    private static class InvertInfo {

        private final JClass<?> jclass;
        private final String fieldPath;
        private final Node expr;

        InvertInfo(JClass<?> jclass, String fieldPath, Node expr) {
            this.jclass = jclass;
            this.fieldPath = fieldPath;
            this.expr = expr;
        }

        public JClass<?> getJClass() {
            return this.jclass;
        }

        public String getFieldPath() {
            return this.fieldPath;
        }

        public Node getExpr() {
            return this.expr;
        }
    }
}

