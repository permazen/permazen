
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;

/**
 * A {@link Node} that evaluates to a {@link Class} object, given the class' name.
 */
public class ClassNode implements Node {

    private final String className;
    private final boolean allowPrimitive;

    /**
     * Constructor.
     *
     * @param className the name of the class
     * @param allowPrimitive whether to allow primitive names like {@code int}
     * @throws IllegalArgumentException if {@code className} is null
     */
    public ClassNode(String className, boolean allowPrimitive) {
        Preconditions.checkArgument(className != null, "null className");
        this.className = className;
        this.allowPrimitive = allowPrimitive;
    }

    @Override
    public Value evaluate(ParseSession session) {
        return new ConstValue(this.resolveClass(session));
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return Class.class;
    }

    /**
     * Attempt to resolve the class name.
     *
     * @throws EvalException if {@code name} cannot be resolved
     */
    public Class<?> resolveClass(ParseSession session) {
        try {
            return session.resolveClass(this.className, this.allowPrimitive);
        } catch (IllegalArgumentException e) {
            throw new EvalException(e.getMessage());
        }
    }

    /**
     * Get the configured class name.
     */
    public String getClassName() {
        return this.className;
    }

    public static ClassNode parse(ParseContext ctx, String name, boolean allowPrimitive) {
        name = name.replaceAll("\\s+", "");
        if (name.startsWith("void[]"))
            throw new ParseException(ctx, "invalid void array type");
        int dims = 0;
        for (int pos = name.length(); pos >= 2 && name.charAt(pos - 2) == '[' && name.charAt(pos - 1) == ']'; pos -= 2)
            dims++;
        if (dims > 255)
            throw new ParseException(ctx, "too many array dimensions (" + dims + " > 255)");
        return new ClassNode(name, allowPrimitive);
    }
}
