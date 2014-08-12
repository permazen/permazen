
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;

import javax.validation.Constraint;

/**
 * Utility routines;
 */
final class Util {

    private static final WildcardType QUESTION_MARK = new WildcardType() {

        @Override
        public Type[] getUpperBounds() {
            return new Type[] { Object.class };
        }

        @Override
        public Type[] getLowerBounds() {
            return new Type[0];
        }

        @Override
        public String toString() {
            return "?";
        }
    };

    private static Method newParameterizedTypeMethod;

    private Util() {
    }

    /**
     * Determine if there is an annotation on {@code thing} with a {@link Constraint} meta-annotation.
     */
    public static boolean requiresValidation(AnnotatedElement thing) {
        if (thing == null)
            throw new IllegalArgumentException("null thing");
        for (Annotation annotation : thing.getAnnotations()) {
            final Class<?> atype = annotation.annotationType();
            if (atype.isAnnotationPresent(Constraint.class))
                return true;
        }
        return false;
    }

    /**
     * Parameterize the raw type with wildcards.
     */
    public static <T> TypeToken<? extends T> getWildcardedType(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        final TypeVariable<Class<T>>[] typeVariables = type.getTypeParameters();
        if (typeVariables.length == 0)
            return TypeToken.of(type);
        final WildcardType[] questionMarks = new WildcardType[typeVariables.length];
        Arrays.fill(questionMarks, QUESTION_MARK);
        return Util.newParameterizedType(type, questionMarks);
    }

    /**
     * Get the n'th generic type parameter.
     */
    public static TypeToken<?> getTypeParameter(TypeToken<?> typeToken, int index) {
        final Type type = typeToken.getType();
        if (!(type instanceof ParameterizedType))
            throw new IllegalArgumentException("type is missing generic type parameter(s)");
        final ParameterizedType parameterizedType = (ParameterizedType)type;
        final Type[] parameters = parameterizedType.getActualTypeArguments();
        if (index >= parameters.length)
            throw new IllegalArgumentException("type is missing generic type parameter(s)");
        return TypeToken.of(parameters[index]);
    }

    /**
     * Invoke method via reflection and re-throw any checked exception wrapped in an {@link JSimpleDBException}.
     */
    public static Object invoke(Method method, Object target, Object... params) {
        try {
            return method.invoke(target, params);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Error)
                throw (Error)e.getCause();
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw new JSimpleDBException("unexpected error invoking method " + method + " on " + target, e);
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JSimpleDBException("unexpected error invoking method " + method + " on " + target, e);
        }
    }

    /**
     * Convert a raw class back into its generic type using caller-supplied type parameters.
     *
     * @param target raw class
     * @param params type parameters
     * @return generic {@link TypeToken} for {@code target}
     * @see <a href="https://code.google.com/p/guava-libraries/issues/detail?id=1645">Guava Issue #1645</a>
     */
    @SuppressWarnings("unchecked")
    private static <T> TypeToken<? extends T> newParameterizedType(Class<T> target, Type[] params) {
        Type type;
        try {
            if (Util.newParameterizedTypeMethod == null) {
                Util.newParameterizedTypeMethod = Class.forName("com.google.common.reflect.Types",
                  false, Thread.currentThread().getContextClassLoader()).getDeclaredMethod(
                  "newParameterizedType", Class.class, Type[].class);
                Util.newParameterizedTypeMethod.setAccessible(true);
            }
            type = (Type)Util.newParameterizedTypeMethod.invoke(null, target, params);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unexpected exception", e);
        }
        return (TypeToken<T>)TypeToken.of(type);
    }
}

