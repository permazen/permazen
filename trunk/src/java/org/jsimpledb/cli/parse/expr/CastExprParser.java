
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import java.util.regex.Matcher;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.util.ParseContext;

public class CastExprParser implements Parser<Node> {

    public static final CastExprParser INSTANCE = new CastExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {

        // Try cast
        final Matcher castMatcher = ctx.tryPattern("\\((" + IdentNode.NAME_PATTERN
          + "(\\s*\\.\\s*" + IdentNode.NAME_PATTERN + ")*)\\s*\\)\\s*");
        if (castMatcher != null) {
            final String className = castMatcher.group(1).replaceAll("\\s", "");
            final Node target = this.parse(session, ctx, complete);             // associates right-to-left
            ctx.skipWhitespace();
            return new Node() {
                @Override
                public Value evaluate(final Session session) {

                    // Evaluate target
                    final Value value = target.evaluate(session);
                    final Object obj = value.get(session);

                    // Handle primitive cast, e.g. "(int)foo"
                    for (Primitive<?> primitive : Primitive.values()) {
                        if (primitive == Primitive.VOID)
                            continue;
                        if (primitive.getName().equals(className)) {
                            if (obj == null)
                                throw new EvalException("invalid cast to " + className + " of null value");
                            if (primitive == Primitive.BOOLEAN)
                                return new Value(new Value(obj).checkBoolean(session, "cast to " + className));
                            final Number num = obj instanceof Character ?
                              (int)(Character)obj : new Value(obj).checkNumeric(session, "cast to " + className);
                            return new Value(primitive.visit(new PrimitiveSwitch<Object>() {
                                @Override
                                public Object caseVoid() {
                                    throw new RuntimeException("internal error");
                                }
                                @Override
                                public Object caseBoolean() {
                                    throw new RuntimeException("internal error");
                                }
                                @Override
                                public Object caseCharacter() {
                                    throw new RuntimeException("internal error");
                                }
                                @Override
                                public Object caseByte() {
                                    return num.byteValue();
                                }
                                @Override
                                public Object caseShort() {
                                    return num.shortValue();
                                }
                                @Override
                                public Object caseInteger() {
                                    return num.intValue();
                                }
                                @Override
                                public Object caseFloat() {
                                    return num.floatValue();
                                }
                                @Override
                                public Object caseLong() {
                                    return num.longValue();
                                }
                                @Override
                                public Object caseDouble() {
                                    return num.doubleValue();
                                }
                            }));
                        }
                    }

                    // Handle regular cast
                    final Class<?> cl = session.resolveClass(className);
                    if (cl == null)
                        throw new EvalException("unknown class `" + className + "'");     // TODO: tab-completions
                    if (obj == null)
                        return new Value(null);
                    if (!cl.isInstance(obj))
                        throw new EvalException("cast to " + className + " failed on object of type " + obj.getClass().getName());
                    return new Value(obj);
                }
            };
        }

        // Parse unary
        return UnaryExprParser.INSTANCE.parse(session, ctx, complete);
    }
}

