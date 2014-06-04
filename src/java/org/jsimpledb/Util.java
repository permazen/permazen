
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.concurrent.ExecutionException;

import javax.validation.Constraint;

import org.jsimpledb.core.ObjId;

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
     * Get the Java model object corresponding to the given object ID from the given object cache.
     *
     * @param cache object cache
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     */
    public static JObject getJObject(LoadingCache<ObjId, JObject> cache, ObjId id) {
        if (id == null)
            throw new IllegalArgumentException("null id");
        Throwable cause;
        try {
            return cache.get(id);
        } catch (ExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        } catch (UncheckedExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        }
        if (cause instanceof JSimpleDBException)
            throw (JSimpleDBException)cause;
        if (cause instanceof Error)
            throw (Error)cause;
        throw new JSimpleDBException("can't instantiate object for ID " + id, cause);
    }
}

