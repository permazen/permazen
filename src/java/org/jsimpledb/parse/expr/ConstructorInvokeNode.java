
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
public class ConstructorInvokeNode extends AbstractInvokeNode<ConstructorExecutable> {

    private final Class<?> klass;

    /**
     * Constructor.
     *
     * @param klass class containing constructor
     * @param paramNodes constructor parameters
     */
    public ConstructorInvokeNode(Class<?> klass, List<Node> paramNodes) {
        super(paramNodes);
        Preconditions.checkArgument(klass != null, "null klass");
        this.klass = klass;
    }

    @Override
    public Value evaluate(final ParseSession session) {

        // Sanity check
        if (this.klass.isPrimitive() || (this.klass.getModifiers() & Modifier.ABSTRACT) != 0)
            throw new EvalException("invalid instantiation of " + this.klass);

        // Evaluate params
        final ParamInfo paramInfo = this.evaluateParams(session);

        // Find matching constructor
        final Constructor<?> constructor = MethodUtil.findMatchingConstructor(this.klass, paramInfo.getParamTypes());
        final ConstructorExecutable executable = new ConstructorExecutable(constructor);

        // Fixup varargs
        this.fixupVarArgs(paramInfo, executable);

        // Fixup type-inferring nodes
        this.fixupTypeInferringNodes(session, paramInfo, executable);

        // Invoke constructor
        final Object result;
        try {
            result = constructor.newInstance(paramInfo.getParams());
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error invoking constructor " + klass.getSimpleName() + "(): " + t, t);
        }

        // Return result value
        return new ConstValue(result);
    }
}
