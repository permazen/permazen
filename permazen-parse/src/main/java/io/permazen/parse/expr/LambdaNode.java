
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.parse.ParseSession;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.List;

/**
 * {@link Node} representing a lambda function.
 */
public class LambdaNode extends TypeInferringNode {

    private final List<Param> params;
    private final Node body;
    private final ThreadLocal<HashMap<String, ValueValue>> frame = new ThreadLocal<>();

    /**
     * Constructor.
     *
     * @param params lambda parameters
     * @param body lambda body
     * @throws IllegalArgumentException if either parameter is null
     */
    public LambdaNode(List<Param> params, Node body) {
        Preconditions.checkArgument(params != null, "null params");
        Preconditions.checkArgument(body != null, "null body");
        this.params = params;
        this.body = body;

        // Associate parameters to this node
        for (Param param : this.params)
            param.setOwner(this);
    }

    @Override
    public <T> Node resolve(ParseSession session, TypeToken<T> type) {

        // Get lookup
        final MethodHandles.Lookup lookup = MethodHandles.lookup();

        // Get handle to this.invoke()
        final MethodType invokeMethodType = MethodType.methodType(Object.class, ParseSession.class, Object[].class);
        final MethodHandle invokeHandle;
        try {
            invokeHandle = lookup.findVirtual(LambdaNode.class, "invoke", invokeMethodType).bindTo(this);
        } catch (IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException("internal error", e);
        }

        // Insert ParseSession as first parameter
        final MethodHandle invokeHandleWithSession = MethodHandles.insertArguments(invokeHandle, 0, session);

        // Enable varargs collection
        final MethodHandle invokeHandleWithSessionAndVarargs = invokeHandleWithSession.asVarargsCollector(Object[].class);

        // Create proxy
        final Object proxy = MethodHandleProxies.asInterfaceInstance(type.getRawType(), invokeHandleWithSessionAndVarargs);

        // Return node containing proxy
        return new ConstNode(new ConstValue(proxy));
    }

    private Object invoke(ParseSession session, Object[] args) throws Throwable {

        // Sanity check
        if ((args != null ? args.length : 0) != this.params.size())
            throw new EvalException("internal error: wrong # params");

        // Set up parameter values
        int index = 0;
        final HashMap<String, ValueValue> paramMap = new HashMap<>();
        for (Param param : this.params)
            paramMap.put(param.getName(), new ValueValue(new ConstValue(args[index++])));

        // Evaluate expression with new parameters in place
        final HashMap<String, ValueValue> previousParams = this.frame.get();
        this.frame.set(paramMap);
        try {
            return this.body.evaluate(session).get(session);
        } finally {
            if (previousParams != null)
                this.frame.set(previousParams);
            else
                this.frame.remove();
        }
    }

// ParamNode

    public static class Param implements Node {

        private final String name;

        private LambdaNode owner;

        public Param(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

        private void setOwner(LambdaNode owner) {
            this.owner = owner;
        }

        @Override
        public Value evaluate(ParseSession session) {
            return this.owner.frame.get().get(this.name);
        }

        @Override
        public Class<?> getType(ParseSession session) {
            return Object.class;
        }
    }
}
