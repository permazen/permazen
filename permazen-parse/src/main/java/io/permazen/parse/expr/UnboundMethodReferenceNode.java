
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

import io.permazen.parse.ParseSession;

/**
 * {@link Node} representing an unbound method reference like {@code String::valueOf} or {@code String::length}.
 */
public class UnboundMethodReferenceNode extends MethodReferenceNode {

    private final ClassNode classNode;

    /**
     * Constructor.
     *
     * @param classNode method class
     * @param name method name
     * @throws IllegalArgumentException if either parameter is null
     */
    public UnboundMethodReferenceNode(ClassNode classNode, String name) {
        super(name);
        Preconditions.checkArgument(classNode != null, "null classNode");
        this.classNode = classNode;
    }

    @Override
    public <T> Node resolve(ParseSession session, TypeToken<T> type) {
        final Class<?> cl = this.classNode.resolveClass(session);
        final Method shape = MethodUtil.findFunctionalMethod(type.getRawType());
        final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        final Type[] ptypes = shape.getGenericParameterTypes();
        try {
            final MethodType shapeType = lookup.unreflect(shape).type();
            final MethodHandle handle;
            if (this.name.equals("new")) {
                if (cl.isArray()) {
                    handle = MethodHandles.insertArguments(lookup.findStatic(Array.class, "newInstance",
                      MethodType.methodType(Object.class, Class.class, int.class)), 0, cl.getComponentType());
                } else {
                    final Constructor<?> constructor = MethodUtil.findMatchingConstructor(cl, ptypes);
                    handle = lookup.unreflectConstructor(constructor).asType(shapeType);
                }
            } else {

                // Lookup public instance and static methods
                Method instanceMethod = this.lookupInstanceMethod(cl, ptypes, shape, false);
                Method staticMethod = this.lookupStaticMethod(cl, ptypes, shape, false);

                // If neither of those were found, try non-public methods
                if (instanceMethod == null && staticMethod == null) {
                    instanceMethod = this.lookupInstanceMethod(cl, ptypes, shape, true);
                    staticMethod = this.lookupStaticMethod(cl, ptypes, shape, true);
                }

                // There must be exactly one match
                if (instanceMethod == null && staticMethod == null)
                    throw new EvalException("method " + this.name + "() not found in " + cl);
                if (instanceMethod != null && staticMethod != null)
                    throw new EvalException("ambiguous invocation of `" + this.name + "()' in " + cl);

                // Get the corresponding method handle
                handle = lookup.unreflect(instanceMethod != null ? instanceMethod : staticMethod);
            }

            // Create proxy
            return new ConstNode(new ConstValue(MethodHandleProxies.asInterfaceInstance(type.getRawType(), handle)));
        } catch (NoSuchMethodException | IllegalAccessException | RuntimeException e) {
            throw new EvalException("failed to resolve method " + cl.getName() + "::" + this.name + " for " + type, e);
        }
    }

    private Method lookupInstanceMethod(Class<?> cl, Type[] ptypes, Method shape, boolean searchNonPublic) {

        // Check first parameter type can match 'this'
        if (ptypes.length == 0)
            return null;
        if (!cl.isAssignableFrom(TypeToken.of(ptypes[0]).getRawType())
          && !(shape.getGenericParameterTypes()[0] instanceof TypeVariable))
            return null;

        // Check remaining method parameter types
        final Type[] mtypes = new Type[ptypes.length - 1];
        System.arraycopy(ptypes, 1, mtypes, 0, mtypes.length);
        try {
            return MethodUtil.findMatchingMethod(cl, this.name, searchNonPublic,
              mtypes, shape.getReturnType() != void.class ? shape.getReturnType() : null, false);
        } catch (EvalException e) {
            return null;
        }
    }

    private Method lookupStaticMethod(Class<?> cl, Type[] ptypes, Method shape, boolean searchNonPublic) {
        try {
            return MethodUtil.findMatchingMethod(cl, this.name, searchNonPublic,
              ptypes, shape.getReturnType() != void.class ? shape.getReturnType() : null, true);
        } catch (EvalException e) {
            return null;
        }
    }
}
