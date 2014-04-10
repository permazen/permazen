
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import org.dellroad.stuff.java.MethodAnnotationScanner;

/**
 * Support superclass for annotation scanners.
 */
class AnnotationScanner<T, A extends Annotation> extends MethodAnnotationScanner<T, A> {

    final JClass<T> jclass;

    @SuppressWarnings("unchecked")
    AnnotationScanner(JClass<T> jclass, Class<A> annotationType) {
        super((Class<T>)jclass.typeToken.getRawType(), annotationType);
        this.jclass = jclass;
    }

    public String getAnnotationDescription() {
        return "@" + this.annotationType.getSimpleName();
    }

    protected void checkNotStatic(Method method) {
        if ((method.getModifiers() & Modifier.STATIC) != 0)
            throw new IllegalArgumentException(this.getErrorPrefix(method) + "annotation is not supported on static methods");
    }

    protected void checkReturnType(Method method, List<TypeToken<?>> expecteds) {
        final TypeToken<?> actual = TypeToken.of(method.getGenericReturnType());
        for (TypeToken<?> expected : expecteds) {
            if (actual.equals(expected))
                return;
        }
        throw new IllegalArgumentException(this.getErrorPrefix(method) + "method is required to return "
          + (expecteds.size() != 1 ? "one of " + expecteds : expecteds.get(0)));
    }

    protected void checkReturnType(Method method, Class<?>... expecteds) {
        final Class<?> actual = method.getReturnType();
        for (Class<?> expected : expecteds) {
            if (actual.equals(expected))
                return;
        }
        throw new IllegalArgumentException(this.getErrorPrefix(method) + "method is required to return "
          + (expecteds.length != 1 ? "one of " + Arrays.asList(expecteds) : expecteds[0]));
    }

    protected void checkParameterTypes(Method method, List<TypeToken<?>> expected) {
        final List<TypeToken<?>> actual = this.getParameterTypeTokens(method);
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException(this.getErrorPrefix(method) + "method is required to take "
              + (expected.isEmpty() ? "zero parameters" : expected.size() + " parameter(s) of type " + expected));
        }
    }

    protected void checkParameterTypes(Method method, TypeToken<?>... expected) {
        this.checkParameterTypes(method, Arrays.asList(expected));
    }

    protected void checkSingleParameterType(Method method, List<TypeToken<?>> choices) {
        final List<TypeToken<?>> actuals = this.getParameterTypeTokens(method);
        if (actuals.size() != 1 || !choices.contains(actuals.get(0))) {
            throw new IllegalArgumentException(this.getErrorPrefix(method) + "method is required to take a single parameter"
              + " with type " + (choices.size() != 1 ? "one of " + choices : choices.get(0)));
        }
    }

    protected List<TypeToken<?>> getParameterTypeTokens(Method method) {
        return Lists.transform(Arrays.asList(method.getGenericParameterTypes()), new Function<Type, TypeToken<?>>() {
            @Override
            public TypeToken<?> apply(Type type) {
                return TypeToken.of(type);
            }
        });
    }

    protected String getErrorPrefix(Method method) {
        return "invalid @" + this.annotationType.getSimpleName() + " annotation on method " + method + ": ";
    }
}

