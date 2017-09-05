
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Array;
import java.lang.reflect.Executable;
import java.lang.reflect.Type;
import java.util.List;

import io.permazen.parse.ParseSession;

/**
 * Superclass of {@link Node}s that invokes a Java method or constructor.
 */
abstract class AbstractInvokeNode<T extends Executable> implements Node {

    final List<Node> paramNodes;

    /**
     * Constructor method invocation.
     *
     * @param paramNodes parameters
     */
    protected AbstractInvokeNode(List<Node> paramNodes) {
        Preconditions.checkArgument(paramNodes != null, "null paramNodes");
        for (Node paramNode : paramNodes)
            Preconditions.checkArgument(paramNode != null, "null paramNode in list");
        this.paramNodes = paramNodes;
    }

    /**
     * Evaluate parameters and parameter types.
     *
     * <p>
     * {@link TypeInferringNode}s are not evaluated, instead they are left as null and the corresponding
     * parameter type is set to the special value {@link MethodUtil.FunctionalType}. Null parameters have
     * their parameter type set to {@link MethodUtil.NullType}.
     *
     * @param session parse session
     * @return evaluated parameter info
     */
    protected ParamInfo evaluateParams(ParseSession session) {
        final Object[] params = new Object[this.paramNodes.size()];
        final Type[] paramTypes = new Type[this.paramNodes.size()];
        for (int i = 0; i < params.length; i++) {
            final Node paramNode = this.paramNodes.get(i);
            if (paramNode instanceof TypeInferringNode) {
                paramTypes[i] = MethodUtil.FunctionalType.class;
                continue;
            }
            params[i] = paramNode.evaluate(session).get(session);
            paramTypes[i] = params[i] == null ? MethodUtil.NullType.class : params[i].getClass();
        }
        return new ParamInfo(params, paramTypes);
    }

    protected void fixupVarArgs(ParamInfo paramInfo, T executable) {

        // Varargs possible?
        if (!executable.isVarArgs())
            return;

        // Compare parameters to what is expected
        final Object[] params = paramInfo.getParams();
        final Class<?>[] formalTypes = executable.getParameterTypes();
        final Class<?> lastFormalType = formalTypes[formalTypes.length - 1];
        final Class<?> lastFormalElemType = lastFormalType.getComponentType();

        // Handle the ambiguous case
        if (params.length == formalTypes.length) {

            // Gather info
            final Object lastParam = params[params.length - 1];
            final Type lastParamType = paramInfo.getParamTypes()[params.length - 1];

            // Is varargs even possible?
            final boolean canVarargs = MethodUtil.isCompatibleMethodParam(lastFormalElemType, lastParamType);
            if (!canVarargs)
                return;

            // Is there ambiguity? XXX should check for an explicit cast one way or the other... when nodes provide their types
            final boolean canNormal = MethodUtil.isCompatibleMethodParam(lastFormalType, lastParamType);
            if (canNormal)
                return;
        }

        // Varargs invocation: collect the trailing arguments into an array
        final int offset = formalTypes.length - 1;
        final int numVarargs = params.length - offset;
        final Object varargs = Array.newInstance(lastFormalElemType, numVarargs);
        for (int i = 0; i < numVarargs; i++) {
            try {
                Array.set(varargs, i, params[offset + i]);
            } catch (Exception e) {
                throw new EvalException("invalid varargs parameter #" + (offset + i + 1) + " to " + executable, e);
            }
        }

        // Replace original varargs parameters with array
        final Object[] newParams = new Object[formalTypes.length];
        System.arraycopy(params, 0, newParams, 0, formalTypes.length - 1);
        newParams[formalTypes.length - 1] = varargs;
        paramInfo.setParams(newParams);
        paramInfo.setVarArgs(true);
    }

    protected void fixupTypeInferringNodes(ParseSession session, ParamInfo paramInfo, T executable) {
        final Object[] params = paramInfo.getParams();
        final Class<?>[] formalTypes = executable.getParameterTypes();
        for (int i = 0; i < params.length; i++) {

            // Resolve varargs array members
            if (i == params.length - 1 && paramInfo.isVarArgs()) {
                final Class<?> lastFormalElemType = formalTypes[formalTypes.length - 1].getComponentType();
                final Object varargs = params[i];
                final int numVarargs = Array.getLength(varargs);
                for (int j = 0; j < numVarargs; j++) {
                    final Node node = this.paramNodes.get(i + j);
                    if (node instanceof TypeInferringNode) {
                        assert Array.get(varargs, j) == null;
                        try {
                            Array.set(varargs, j, this.resolveAndEvaluate(session, (TypeInferringNode)node, lastFormalElemType));
                        } catch (Exception e) {
                            throw new EvalException("invalid varargs parameter #" + (i + j + 1) + " to " + executable, e);
                        }
                    }
                }
            }

            // Resolve parameter
            if (i < this.paramNodes.size()) {
                final Node paramNode = this.paramNodes.get(i);
                if (paramNode instanceof TypeInferringNode) {
                    assert params[i] == null;
                    params[i] = this.resolveAndEvaluate(session, (TypeInferringNode)paramNode, formalTypes[i]);
                }
            }
        }
    }

    private Object resolveAndEvaluate(ParseSession session, TypeInferringNode node, Class<?> type) {
        return node.resolve(session, TypeToken.of(type)).evaluate(session).get(session);
    }

// ParamInfo

    static class ParamInfo {

        private Object[] params;
        private Type[] paramTypes;
        private boolean varargs;

        ParamInfo(Object[] params, Type[] paramTypes) {
            this.params = params;
            this.paramTypes = paramTypes;
        }

        public Object[] getParams() {
            return this.params;
        }
        public void setParams(Object[] params) {
            this.params = params;
        }

        public Type[] getParamTypes() {
            return this.paramTypes;
        }
        public void setParamTypes(Type[] paramTypes) {
            this.paramTypes = paramTypes;
        }

        public boolean isVarArgs() {
            return this.varargs;
        }
        public void setVarArgs(boolean varargs) {
            this.varargs = varargs;
        }
    }
}
