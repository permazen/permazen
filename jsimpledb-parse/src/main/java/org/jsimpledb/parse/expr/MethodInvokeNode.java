
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Node} that invokes a Java method when evaluated.
 */
public class MethodInvokeNode extends AbstractInvokeNode<MethodExecutable> {

    private final Class<?> klass;
    private final Node targetNode;
    private final String name;

    /**
     * Constructor for static method invocation.
     *
     * @param klass class containing static method
     * @param name static method name
     * @param paramNodes method parameters
     */
    public MethodInvokeNode(Class<?> klass, String name, List<Node> paramNodes) {
        this(name, paramNodes, klass, null);
        Preconditions.checkArgument(klass != null, "null klass");
    }

    /**
     * Constructor for instance method invocation.
     *
     * @param targetNode node evaluating to target object
     * @param name instance method name
     * @param paramNodes method parameters
     */
    public MethodInvokeNode(Node targetNode, String name, List<Node> paramNodes) {
        this(name, paramNodes, null, targetNode);
        Preconditions.checkArgument(targetNode != null, "null targetNode");
    }

    private MethodInvokeNode(String name, List<Node> paramNodes, Class<?> klass, Node targetNode) {
        super(paramNodes);
        Preconditions.checkArgument(name != null, "null name");
        this.klass = klass;
        this.targetNode = targetNode;
        this.name = name;
    }

    @Override
    public Value evaluate(final ParseSession session) {

        // Evaluate invocation target, if any
        final Object target = this.klass == null ?
          this.targetNode.evaluate(session).checkNotNull(session, "method " + name + "() invocation") : null;

        // Evaluate params
        final ParamInfo paramInfo = this.evaluateParams(session);

        // Find matching method
        final Method method = MethodUtil.findMatchingMethod(
          target != null ? target.getClass() : this.klass, this.name, paramInfo.getParamTypes(), null, this.klass != null);
        final MethodExecutable executable = new MethodExecutable(method);

        // Fixup varargs
        this.fixupVarArgs(paramInfo, executable);

        // Fixup type-inferring nodes
        this.fixupTypeInferringNodes(session, paramInfo, executable);

        // Invoke method
        final Object result;
        try {
            result = method.invoke(target, paramInfo.getParams());
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error invoking method `" + method.getName() + "()' on "
              + (target != null ? "object of type " + target.getClass().getName() : method.getDeclaringClass()) + ": " + t, t);
        }

        // Return result value
        return result != null || method.getReturnType() != Void.TYPE ? new ConstValue(result) : Value.NO_VALUE;
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return Object.class;
    }
}
