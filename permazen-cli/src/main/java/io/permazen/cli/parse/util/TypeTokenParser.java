
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse.util;

import com.google.common.reflect.TypeToken;

import io.permazen.util.ApplicationClassLoader;
import io.permazen.util.ParseContext;
import io.permazen.util.TypeTokens;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.regex.Matcher;

/**
 * Recreates {@link TypeToken}s from the output of {@link TypeToken#toString}.
 * Currently requires that no type variables or wildcards appear.
 *
 * @see <a href="https://github.com/google/guava/issues/1645">Guava issue #1645</a>
 */
public class TypeTokenParser {

    private final ClassLoader loader;

    /**
     * Default constructor.
     */
    public TypeTokenParser() {
        this(null);
    }

    /**
     * Primary constructor.
     *
     * @param loader class loader to use for loading classes, or null for the {@link ApplicationClassLoader}
     */
    public TypeTokenParser(ClassLoader loader) {
        if (loader == null)
            loader = ApplicationClassLoader.getInstance();
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
            throw new IllegalArgumentException(String.format("string contains trailing garbage at index %d", ctx.getIndex()));
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
                if (ch != ',') {
                    throw new IllegalArgumentException(String.format(
                      "unexpected character \"%c\" at index %d", (char)ch, ctx.getIndex()));
                }
            }
        }
        if (parameterTypes.size() != typeParameters.length) {
            throw new IllegalArgumentException(String.format(
              "%s has %d generic type parameter(s) but %d parameter(s) were supplied",
              cl, typeParameters.length, parameterTypes.size()));
        }
        return parameterTypes.isEmpty() ?
          TypeToken.of(cl) : TypeTokens.newParameterizedType(cl, parameterTypes.toArray(new Type[0]));
    }

// Test method

    public static void main(String[] args) throws Exception {
        final TypeTokenParser parser = new TypeTokenParser();
        for (String arg : args)
            System.out.println(parser.parse(new ParseContext(arg)));
    }
}
