
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitchAdapter;
import io.permazen.core.FieldType;
import io.permazen.parse.ParseSession;

/**
 * A parsed cast expression.
 *
 * <p>
 * Includes an extension to support casting a {@link String} to any supported field type
 * via {@link FieldType#fromString}, for example {@code (java.time.Duration)"PT24H"}.
 */
public class CastNode implements Node {

    private final ClassNode typeNode;
    private final Node target;

    /**
     * Constructor.
     *
     * @param typeNode cast type node
     * @param target cast target
     */
    public CastNode(ClassNode typeNode, Node target) {
        Preconditions.checkArgument(typeNode != null, "null typeNode");
        Preconditions.checkArgument(target != null, "null target");
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
            obj = target.evaluate(session).get(session);                            // just evaluate target

        // Check for null
        if (obj == null) {
            if (type.isPrimitive())
                throw new EvalException("invalid cast of null value to " + typeName);
            return new ConstValue(null);
        }

        // Check for cast of a String to a supported field type (other than String)
        if (obj instanceof String && type != String.class && session.getDatabase() != null) {
            final FieldType<?> fieldType = session.getDatabase().getFieldTypeRegistry().getFieldType(TypeToken.of(type));
            if (fieldType != null) {
                final Object value;
                try {
                    return new ConstValue(fieldType.fromString((String)obj));
                } catch (IllegalArgumentException e) {
                    // nope
                }
            }
        }

        // Handle primitive cast, e.g. "(int)foo"
        if (type.isPrimitive()) {
            final Primitive<?> primitive = Primitive.forName(typeName);

            // Handle cast
            if (primitive == Primitive.BOOLEAN)
                return new ConstValue(new ConstValue(obj).checkBoolean(session, "cast to " + typeName));
            final Number num = obj instanceof Character ?
              (int)(Character)obj : new ConstValue(obj).checkNumeric(session, "cast to " + typeName);
            return new ConstValue(primitive.visit(new PrimitiveSwitchAdapter<Object>() {
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
                @Override
                protected Object caseDefault() {
                    throw new RuntimeException("internal error");
                }
            }));
        }

        // Cast it
        if (!type.isInstance(obj))
            throw new EvalException("can't cast object of type " + obj.getClass().getName() + " to " + typeName);
        return new ConstValue(obj);
    }
}
