
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import com.google.common.collect.Lists;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.regex.Matcher;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.util.ParseContext;

public class NewCastExprParser implements Parser<Node> {

    public static final NewCastExprParser INSTANCE = new NewCastExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {

        // Try new
        final Matcher newMatcher = ctx.tryPattern("new\\s+(" + IdentNode.NAME_PATTERN
          + "(\\s*\\.\\s" + IdentNode.NAME_PATTERN + ")*)\\s*\\(");
        if (newMatcher != null) {
            final String className = newMatcher.group(1).replaceAll("\\s", "");
            final Class<?> cl = session.resolveClass(className);
            if (cl == null)
                throw new ParseException(ctx, "unknown class `" + className + "'");     // TODO: tab-completions
            final List<Node> paramNodes = BaseExprParser.parseParams(session, ctx, complete);
            return new Node() {
                @Override
                public Value evaluate(final Session session) {
                    if (cl.isInterface())
                        throw new EvalException("invalid instantiation of " + cl);
                    final Object[] params = Lists.transform(paramNodes, new com.google.common.base.Function<Node, Object>() {
                        @Override
                        public Object apply(Node param) {
                            return param.evaluate(session).get(session);
                        }
                    }).toArray();
                    for (Constructor<?> constructor : cl.getConstructors()) {
                        final Class<?>[] ptypes = constructor.getParameterTypes();
                        if (ptypes.length != params.length)
                            continue;
                        try {
                            return new Value(constructor.newInstance(params));
                        } catch (IllegalArgumentException e) {
                            continue;                               // wrong method, a parameter type didn't match
                        } catch (Exception e) {
                            final Throwable t = e instanceof InvocationTargetException ?
                              ((InvocationTargetException)e).getTargetException() : e;
                            throw new EvalException("error invoking constructor `" + cl.getName() + "()': " + t, t);
                        }
                    }
                    throw new EvalException("no compatible constructor found in class " + cl.getName());
                }
            };
        }

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

