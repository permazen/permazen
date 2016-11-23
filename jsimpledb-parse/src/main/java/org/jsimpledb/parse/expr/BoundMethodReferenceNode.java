
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Node} representing a bound method reference like {@code "foobar"::indexOf}.
 */
public class BoundMethodReferenceNode extends MethodReferenceNode {

    private final Node node;

    /**
     * Constructor.
     *
     * @param node target instance node
     * @param name method name
     * @throws IllegalArgumentException if either parameter is null
     */
    public BoundMethodReferenceNode(Node node, String name) {
        super(name);
        Preconditions.checkArgument(node != null, "null node");
        this.node = node;
    }

    @Override
    public <T> Node resolve(ParseSession session, TypeToken<T> type) {

        // Evaluate target instance
        final Object target = this.node.evaluate(session).checkNotNull(session, "method " + name + "() invocation");

        // Resolve instance method
        final Method shape = MethodUtil.findFunctionalMethod(type.getRawType());
        final Method method = MethodUtil.findMatchingMethod(target.getClass(), this.name, true,
          shape.getGenericParameterTypes(), shape.getReturnType() != void.class ? shape.getReturnType() : null, false);

        // Create proxy
        final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        try {
            final MethodHandle handle = lookup.unreflect(method).bindTo(target);
            return new ConstNode(new ConstValue(MethodHandleProxies.asInterfaceInstance(type.getRawType(), handle)));
        } catch (Exception e) {
            throw new EvalException("failed to resolve method `"
              + this.name + "' in object of type " + target.getClass().getName(), e);
        }
    }
}
