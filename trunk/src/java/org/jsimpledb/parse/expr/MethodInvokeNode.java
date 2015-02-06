
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Node} that invokes a Java method when evaluated.
 */
public class MethodInvokeNode implements Node {

    private final Class<?> klass;
    private final Node node;
    private final String name;
    private final List<Node> paramNodes;

    /**
     * Constructor for static method invocation.
     *
     * @param klass class containing static method
     * @param name static method name
     * @param paramNodes method parameters
     */
    public MethodInvokeNode(Class<?> klass, String name, List<Node> paramNodes) {
        this(name, paramNodes, klass, null);
        if (klass == null)
            throw new IllegalArgumentException("null klass");
    }

    /**
     * Constructor for instance method invocation.
     *
     * @param node node evaluating to target object
     * @param name instance method name
     * @param paramNodes method parameters
     */
    public MethodInvokeNode(Node node, String name, List<Node> paramNodes) {
        this(name, paramNodes, null, node);
        if (node == null)
            throw new IllegalArgumentException("null node");
    }

    private MethodInvokeNode(String name, List<Node> paramNodes, Class<?> klass, Node node) {
        if (name == null)
            throw new IllegalArgumentException("null target");
        if (paramNodes == null)
            throw new IllegalArgumentException("null paramNodes");
        for (Node paramNode : paramNodes) {
            if (paramNode == null)
                throw new IllegalArgumentException("null paramNode in list");
        }
        this.klass = klass;
        this.node = node;
        this.name = name;
        this.paramNodes = paramNodes;
    }

    @Override
    public Value evaluate(final ParseSession session) {

        // Evaluate params
        final Object[] params = Lists.transform(paramNodes, new Function<Node, Object>() {
            @Override
            public Object apply(Node param) {
                return param.evaluate(session).get(session);
            }
        }).toArray();

        // Handle static method
        if (this.klass != null)
            return this.invokeMethod(this.klass, null, name, params);

        // Handle instance method
        final Object obj = this.node.evaluate(session).checkNotNull(session, "method " + name + "() invocation");
        return this.invokeMethod(obj.getClass(), obj, name, params);
    }

    private Value invokeMethod(Class<?> cl, Object obj, String name, Object[] params) {

        // Try interface methods
        for (Class<?> iface : this.addInterfaces(cl, new LinkedHashSet<Class<?>>())) {
            for (Method method : iface.getMethods()) {
                final Value value = this.tryMethod(method, obj, name, params);
                if (value != null)
                    return value;
            }
        }

        // Try class methods
        for (Method method : cl.getMethods()) {
            final Value value = this.tryMethod(method, obj, name, params);
            if (value != null)
                return value;
        }

        // Not found
        throw new EvalException("no compatible method `" + name + "()' found in " + cl);
    }

    private Set<Class<?>> addInterfaces(Class<?> cl, Set<Class<?>> interfaces) {
        for (Class<?> iface : cl.getInterfaces()) {
            interfaces.add(iface);
            this.addInterfaces(iface, interfaces);
        }
        if (cl.getSuperclass() != null)
            this.addInterfaces(cl.getSuperclass(), interfaces);
        return interfaces;
    }

    private Value tryMethod(Method method, Object obj, String name, Object[] params) {
        if (!method.getName().equals(name))
            return null;
        final Class<?>[] ptypes = method.getParameterTypes();
        if (method.isVarArgs()) {
            if (params.length < ptypes.length - 1)
                return null;
            Object[] newParams = new Object[ptypes.length];
            System.arraycopy(params, 0, newParams, 0, ptypes.length - 1);
            Object[] varargs = new Object[params.length - (ptypes.length - 1)];
            System.arraycopy(params, ptypes.length - 1, varargs, 0, varargs.length);
            newParams[ptypes.length - 1] = varargs;
            params = newParams;
        } else if (params.length != ptypes.length)
            return null;
        final Object result;
        try {
            result = method.invoke(obj, params);
        } catch (IllegalArgumentException e) {
            return null;                            // a parameter type didn't match -> wrong method
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ?
              ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error invoking method `" + name + "()' on "
              + (obj != null ? "object of type " + obj.getClass().getName() : method.getDeclaringClass()) + ": " + t, t);
        }
        return result != null || method.getReturnType() != Void.TYPE ? new ConstValue(result) : Value.NO_VALUE;
    }
}

