
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.util;

import com.google.common.reflect.TypeToken;

import io.permazen.util.ParseContext;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Recreates {@link TypeToken}s from the output of {@link TypeToken#toString}.
 * Currently requires that no type variables or wildcards appear.
 */
public class TypeTokenParser {

    private final ClassLoader loader;

    private Method newParameterizedTypeMethod;

    /**
     * Default constructor.
     */
    public TypeTokenParser() {
        this(null);
    }

    /**
     * Primary constructor.
     *
     * @param loader class loader to use for loading classes,
     *  or null for the current thread's {@linkplain Thread#getContextClassLoader context loader}
     */
    public TypeTokenParser(ClassLoader loader) {
        if (loader == null)
            loader = Thread.currentThread().getContextClassLoader();
        this.loader = loader;
    }

    /**
     * Parse a {@link TypeToken} in string form.
     *
     * @param string string to parse
     * @return parsed type
     * @throws IllegalArgumentException if the input is invalid
     * @throws ClassNotFoundException if a named class could not be found
     */
    public TypeToken<?> parse(String string) throws ClassNotFoundException {
        final ParseContext ctx = new ParseContext(string);
        final TypeToken<?> token = this.parse(new ParseContext(string));
        ctx.skipWhitespace();
        if (!ctx.isEOF())
            throw new IllegalArgumentException("string contains trailing garbage at index " + ctx.getIndex());
        return token;
    }

    /**
     * Parse a {@link TypeToken} in string form from a {@link ParseContext}.
     * Since the {@link TypeToken} string format is self-delimiting, not all of the input may be consumed.
     *
     * @param ctx parse context
     * @return parsed type
     * @throws IllegalArgumentException if the input is invalid
     * @throws ClassNotFoundException if a named class could not be found
     */
    public TypeToken<?> parse(ParseContext ctx) throws ClassNotFoundException {
        ctx.skipWhitespace();
        final Matcher matcher = ctx.matchPrefix("[^,<>]+");
        final String className = matcher.group();
        final Class<?> cl = this.loader.loadClass(className);
        final TypeVariable<?>[] typeParameters = cl.getTypeParameters();
        ctx.skipWhitespace();
        if (ctx.isEOF())
            return TypeToken.of(cl);
        ctx.skipWhitespace();
        final ArrayList<Type> parameterTypes = new ArrayList<>(typeParameters.length);
        if (ctx.peek() == '<') {
            ctx.read();
            while (true) {
                ctx.skipWhitespace();
                parameterTypes.add(this.parse(ctx).getType());
                ctx.skipWhitespace();
                final int ch = ctx.read();
                if (ch == '>')
                    break;
                if (ch != ',')
                    throw new IllegalArgumentException("unexpected character `" + ch + "' at index " + ctx.getIndex());
            }
        }
        if (parameterTypes.size() != typeParameters.length) {
            throw new IllegalArgumentException(cl + " has " + typeParameters.length + " generic type parameters but "
              + parameterTypes.size() + " parameters were supplied");
        }
        return parameterTypes.isEmpty() ? TypeToken.of(cl) : this.newParameterizedType(cl, parameterTypes);
    }

    /**
     * Convert a raw class back into its generic type.
     *
     * @param target raw class
     * @param params type parameters
     * @return generic {@link TypeToken} for {@code target}
     * @see <a href="https://github.com/google/guava/issues/1645">Guava Issue #1645</a>
     */
    @SuppressWarnings("unchecked")
    private <T> TypeToken<? extends T> newParameterizedType(Class<T> target, List<Type> params) {
        Type type;
        try {
            if (this.newParameterizedTypeMethod == null) {
                this.newParameterizedTypeMethod = Class.forName("com.google.common.reflect.Types", false, this.loader)
                  .getDeclaredMethod("newParameterizedType", Class.class, Type[].class);
                this.newParameterizedTypeMethod.setAccessible(true);
            }
            type = (Type)this.newParameterizedTypeMethod.invoke(null, target, params.toArray(new Type[params.size()]));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unexpected exception", e);
        }
        return (TypeToken<T>)TypeToken.of(type);
    }

// Test method

    public static void main(String[] args) throws Exception {
        final TypeTokenParser parser = new TypeTokenParser();
        for (String arg : args)
            System.out.println(parser.parse(new ParseContext(arg)));
    }
}

