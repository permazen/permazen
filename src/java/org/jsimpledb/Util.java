
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

import javax.validation.Constraint;

import org.jsimpledb.core.DatabaseException;

/**
 * Utility routines;
 */
final class Util {

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
     * Invoke method via reflection and re-throw any checked exception wrapped in an {@link DatabaseException}.
     */
    public static Object invoke(Method method, Object target, Object... params) {
        try {
            return method.invoke(target, params);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Error)
                throw (Error)e.getCause();
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw new DatabaseException("unexpected error invoking method " + method + " on " + target, e);
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseException("unexpected error invoking method " + method + " on " + target, e);
        }
    }
}

