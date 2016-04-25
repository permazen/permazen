
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.reflect.TypeToken;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;
import org.jsimpledb.parse.ParseSession;

/**
 * A parsed cast expression.
 */
public class CastNode implements Node {

    private final ClassNode typeNode;
    private final Node target;

    /**
     * Constructor.
     *
     * @param typeNode cast type node
     * @param initialValue array initial value; each element in the list is either a {@link List} (all but the last dimension)
     *  or a {@code Node} (last dimension)
     */
    public CastNode(ClassNode typeNode, Node target) {
        this.typeNode = typeNode;
        this.target = target;
    }

    @Override
    public Class<?> getType(ParseSession session) {
        try {
            return this.typeNode.resolveClass(session);
        } catch (EvalException e) {
            return Object.class;
        }
    }

    @Override
    public Value evaluate(final ParseSession session) {

        // Evaluate type
        final Class<?> type = this.typeNode.resolveClass(session);
        if (type == void.class)
            throw new EvalException("illegal cast to void");
        final String typeName = this.typeNode.getClassName();

        // Evaluate target, properly handling type-inferring nodes (e.g., lambdas and method references)
        final Object obj;
        if (this.target instanceof TypeInferringNode) {                              // evaluate target in context
            final Node node = ((TypeInferringNode)this.target).resolve(session, TypeToken.of(type));
            obj = node.evaluate(session).get(session);
        } else
            obj = target.evaluate(session).get(session);                        // just evaluate target

        // Handle primitive cast, e.g. "(int)foo"
        if (type.isPrimitive()) {
            final Primitive<?> primitive = Primitive.forName(typeName);

            // Check for null
            if (target == null)
                throw new EvalException("invalid cast of null value to " + typeName);

            // Handle cast
            if (primitive == Primitive.BOOLEAN)
                return new ConstValue(new ConstValue(obj).checkBoolean(session, "cast to " + typeName));
            final Number num = obj instanceof Character ?
              (int)(Character)obj : new ConstValue(obj).checkNumeric(session, "cast to " + typeName);
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
        if (obj != null && !type.isInstance(obj))
            throw new EvalException("can't cast object of type " + obj.getClass().getName() + " to " + typeName);
        return new ConstValue(obj);
    }
}
