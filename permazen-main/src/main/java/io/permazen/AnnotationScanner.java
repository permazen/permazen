
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import org.dellroad.stuff.java.MethodAnnotationScanner;

/**
 * Support superclass for Java model class annotation scanners.
 */
abstract class AnnotationScanner<T, A extends Annotation> extends MethodAnnotationScanner<T, A> {

    /**
     * The associated Java model class, if any.
     */
    protected final JClass<T> jclass;

    /**
     * Constructor for when there is an associated {@link JClass}.
     *
     * @param jclass Java model class
     * @param annotationType annotation to scan for
     */
    protected AnnotationScanner(JClass<T> jclass, Class<A> annotationType) {
        super(jclass.getType(), annotationType);
        this.jclass = jclass;
    }

    /**
     * Constructor for when there is no associated {@link JClass}.
     *
     * @param type Java type to scan
     * @param annotationType annotation to scan for
     */
    @SuppressWarnings("unchecked")
    protected AnnotationScanner(Class<T> type, Class<A> annotationType) {
        super(type, annotationType);
        this.jclass = null;
    }

    /**
     * Get the annotation on the given method.
     *
     * <p>
     * Utilizes {@link Util#getAnnotation Util.getAnnotation()}.
     *
     * @return the annotation found, or null if not found
     */
    @Override
    protected A getAnnotation(Method method) {
        return Util.getAnnotation(method, this.annotationType);
    }

    /**
     * Get a simple description of the annotation being scanned for.
     *
     * @return annotation description
     */
    public String getAnnotationDescription() {
        return "@" + this.annotationType.getSimpleName();
    }

    /**
     * Verify method is not static.
     *
     * @param method the method to check
     * @throws IllegalArgumentException if method is static
     */
    protected void checkNotStatic(Method method) {
        if ((method.getModifiers() & Modifier.STATIC) != 0) {
            throw new IllegalArgumentException(String.format(
              "%s: annotation is not supported on static methods", this.getErrorPrefix(method)));
        }
    }

    /**
     * Verify method return type.
     *
     * @param method the method to check
     * @param expecteds allowed return types
     * @throws IllegalArgumentException if has an invalid return type
     */
    protected void checkReturnType(Method method, List<TypeToken<?>> expecteds) {
        final TypeToken<?> actual = TypeToken.of(method.getGenericReturnType());
        for (TypeToken<?> expected : expecteds) {
            if (actual.equals(expected))
                return;
        }
        throw new IllegalArgumentException(String.format("%s: method is required to return %s but instead returns %s",
          this.getErrorPrefix(method), expecteds.size() != 1 ? "one of " + expecteds : expecteds.get(0), actual));
    }

    /**
     * Verify method return type.
     *
     * @param method the method to check
     * @param expecteds allowed return types
     * @throws IllegalArgumentException if has an invalid return type
     */
    protected void checkReturnType(Method method, Class<?>... expecteds) {
        final Class<?> actual = method.getReturnType();
        for (Class<?> expected : expecteds) {
            if (actual.equals(expected))
                return;
        }
        throw new IllegalArgumentException(String.format("%s: method is required to return %s but instead returns %s",
          this.getErrorPrefix(method), expecteds.length != 1 ? "one of " + Arrays.asList(expecteds) : expecteds[0], actual));
    }

    /**
     * Verify method parameter type(s).
     *
     * @param method the method to check
     * @param expected expected parameter types
     * @throws IllegalArgumentException if has an invalid parameter type(s)
     */
    protected void checkParameterTypes(Method method, List<TypeToken<?>> expected) {
        final List<TypeToken<?>> actual = this.getParameterTypeTokens(method);
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException(String.format("%s: method is required to take %s",
              this.getErrorPrefix(method),
              expected.isEmpty() ? "zero parameters" : expected.size() + " parameter(s) of type " + expected));
        }
    }

    /**
     * Verify method parameter type(s).
     *
     * @param method the method to check
     * @param expected expected parameter types
     * @throws IllegalArgumentException if has an invalid parameter type(s)
     */
    protected void checkParameterTypes(Method method, TypeToken<?>... expected) {
        this.checkParameterTypes(method, Arrays.asList(expected));
    }

    /**
     * Verify a specific method parameter's type.
     *
     * @param method the method to check
     * @param index parameter index
     * @param choices allowed parameter types
     * @throws IllegalArgumentException if parameter type does not match
     */
    protected void checkParameterType(Method method, int index, List<TypeToken<?>> choices) {
        final List<TypeToken<?>> actuals = this.getParameterTypeTokens(method);
        if (actuals.size() <= index || !choices.contains(actuals.get(index))) {
            throw new IllegalArgumentException(String.format("%s: method parameter #%d is required to have type %s",
              this.getErrorPrefix(method), index + 1, choices.size() != 1 ? "one of " + choices : choices.get(0)));
        }
    }

    /**
     * Verify a specific method parameter's type.
     *
     * @param method the method to check
     * @param index parameter index
     * @param choices allowed parameter types
     * @throws IllegalArgumentException if parameter type does not match
     */
    protected void checkParameterType(Method method, int index, TypeToken<?>... choices) {
        this.checkParameterType(method, index, Arrays.asList(choices));
    }

    /**
     * Verify method takes a single parameter of the specified type.
     *
     * @param method the method to check
     * @param choices allowed parameter types
     * @throws IllegalArgumentException if parameter type does not match
     */
    protected void checkSingleParameterType(Method method, List<TypeToken<?>> choices) {
        final List<TypeToken<?>> actuals = this.getParameterTypeTokens(method);
        if (actuals.size() != 1 || !choices.contains(actuals.get(0))) {
            throw new IllegalArgumentException(String.format("%s: method is required to take a single parameter with type %s",
              this.getErrorPrefix(method), choices.size() != 1 ? "one of " + choices : choices.get(0)));
        }
    }

    /**
     * Get method parameter types as {@link TypeToken}s.
     *
     * @param method the method to check
     * @return method parameter types
     */
    protected List<TypeToken<?>> getParameterTypeTokens(Method method) {
        return Lists.transform(Arrays.asList(method.getGenericParameterTypes()), TypeToken::of);
    }

    /**
     * Get "invalid annotation" error message prefix that describes the annotation on the specified method.
     *
     * @param method the method to check
     * @return error message prefix
     */
    protected String getErrorPrefix(Method method) {
        return String.format("invalid %s annotation on method %s for type \"%s\"",
          this.getAnnotationDescription(), method, this.jclass.getName());
    }
}
