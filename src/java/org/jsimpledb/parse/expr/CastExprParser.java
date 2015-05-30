
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.util.regex.Matcher;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;

/**
 * Parses type cast expressions.
 */
public class CastExprParser implements Parser<Node> {

    public static final CastExprParser INSTANCE = new CastExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Try cast
        final int start = ctx.getIndex();
        final Matcher castMatcher = ctx.tryPattern("\\((" + IdentNode.NAME_PATTERN
          + "(\\s*\\.\\s*" + IdentNode.NAME_PATTERN + ")*)\\s*\\)\\s*");
        if (castMatcher != null) {
            final String className = castMatcher.group(1).replaceAll("\\s", "");
            if (className.equals("null")) {
                ctx.setIndex(start);
                return UnaryExprParser.INSTANCE.parse(session, ctx, complete);
            }
            final Node target = this.parse(session, ctx, complete);             // associates right-to-left
            ctx.skipWhitespace();
            return new Node() {
                @Override
                public Value evaluate(final ParseSession session) {

                    // Evaluate target
                    final Object obj = target.evaluate(session).get(session);

                    // Handle primitive cast, e.g. "(int)foo"
                    final Primitive<?> primitive = Primitive.forName(className);
                    if (primitive != null && primitive != Primitive.VOID) {
                        if (obj == null)
                            throw new EvalException("invalid cast of null value to " + className);
                        if (primitive == Primitive.BOOLEAN)
                            return new ConstValue(new ConstValue(obj).checkBoolean(session, "cast to " + className));
                        final Number num = obj instanceof Character ?
                          (int)(Character)obj : new ConstValue(obj).checkNumeric(session, "cast to " + className);
                        return new ConstValue(primitive.visit(new PrimitiveSwitch<Object>() {
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

                    // Handle regular cast
                    final Class<?> cl = session.resolveClass(className);
                    if (cl == null)
                        throw new EvalException("unknown class `" + className + "'");     // TODO: tab-completions
                    if (obj != null && !cl.isInstance(obj))
                        throw new EvalException("can't cast object of type " + obj.getClass().getName() + " to " + className);
                    return new ConstValue(obj);
                }
            };
        }

        // Parse unary
        return UnaryExprParser.INSTANCE.parse(session, ctx, complete);
    }
}

