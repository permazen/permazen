
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.reflect.TypeToken;

import java.util.regex.Matcher;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
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
        final Matcher castMatcher = ctx.tryPattern("\\(\\s*(" + LiteralExprParser.CLASS_NAME_PATTERN + ")\\s*\\)\\s*");
        while (castMatcher != null) {
            final String className = castMatcher.group(1).replaceAll("\\s", "");

            // Resolve class name
            final Class<?> cl;
            try {
                cl = session.resolveClass(className, true, true);
            } catch (IllegalArgumentException e) {
                ctx.setIndex(start);
                break;
            }

            // Check for void cast
            if (cl == void.class)
                throw new ParseException(ctx, "illegal cast to " + className);

            // Parse targe of cast
            final Node target = this.parse(session, ctx, complete);             // associates right-to-left
            ctx.skipWhitespace();

            // Return casting node
            return new CastNode(cl, className, target);
        }

        // Try lambda
        try {
            return LambdaExprParser.INSTANCE.parse(session, ctx, complete);
        } catch (ParseException e) {
            ctx.setIndex(start);
        }

        // Parse unary
        return UnaryExprParser.INSTANCE.parse(session, ctx, complete);
    }

// CastNode

    private static class CastNode implements Node {

        private final Class<?> type;
        private final String typeName;
        private final Node target;

        CastNode(Class<?> type, String typeName, Node target) {
            this.type = type;
            this.typeName = typeName;
            this.target = target;
        }

        public Value evaluate(final ParseSession session) {

            // Evaluate target, properly handling type-inferring nodes (e.g., lambdas and method references)
            final Object obj;
            if (this.target instanceof TypeInferringNode) {                              // evaluate target in context
                final Node node = ((TypeInferringNode)this.target).resolve(session, TypeToken.of(this.type));
                obj = node.evaluate(session).get(session);
            } else
                obj = target.evaluate(session).get(session);                        // just evaluate target

            // Handle primitive cast, e.g. "(int)foo"
            if (this.type.isPrimitive()) {
                final Primitive<?> primitive = Primitive.forName(this.typeName);

                // Check for null
                if (target == null)
                    throw new EvalException("invalid cast of null value to " + this.typeName);

                // Handle cast
                if (primitive == Primitive.BOOLEAN)
                    return new ConstValue(new ConstValue(obj).checkBoolean(session, "cast to " + this.typeName));
                final Number num = obj instanceof Character ?
                  (int)(Character)obj : new ConstValue(obj).checkNumeric(session, "cast to " + this.typeName);
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

            // Cast it
            if (obj != null && !this.type.isInstance(obj))
                throw new EvalException("can't cast object of type " + obj.getClass().getName() + " to " + this.typeName);
            return new ConstValue(obj);
        }
    };
}
