
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Node} that invokes a Java method when evaluated.
 */
public class ConstructorInvokeNode extends AbstractInvokeNode<Constructor<?>> {

    private final ClassNode classNode;

    /**
     * Constructor.
     *
     * @param classNode class containing constructor
     * @param paramNodes constructor parameters
     */
    public ConstructorInvokeNode(ClassNode classNode, List<Node> paramNodes) {
        super(paramNodes);
        Preconditions.checkArgument(classNode != null, "null classNode");
        this.classNode = classNode;
    }

    @Override
    public Value evaluate(final ParseSession session) {

        // Resolve class
        final Class<?> cl = this.classNode.resolveClass(session);

        // Sanity check
        if (cl.isPrimitive() || (cl.getModifiers() & Modifier.ABSTRACT) != 0)
            throw new EvalException("invalid instantiation of " + cl);

        // Evaluate params
        final ParamInfo paramInfo = this.evaluateParams(session);

        // Find matching constructor
        final Constructor<?> constructor = MethodUtil.findMatchingConstructor(cl, paramInfo.getParamTypes());

        // Fixup varargs
        this.fixupVarArgs(paramInfo, constructor);

        // Fixup type-inferring nodes
        this.fixupTypeInferringNodes(session, paramInfo, constructor);

        // Invoke constructor
        final Object result;
        try {
            result = constructor.newInstance(paramInfo.getParams());
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error invoking constructor " + cl.getSimpleName() + "(): " + t, t);
        }

        // Return result value
        return new ConstValue(result);
    }

    @Override
    public Class<?> getType(ParseSession session) {
        try {
            return this.classNode.resolveClass(session);
        } catch (EvalException e) {
            return Object.class;
        }
    }
}
