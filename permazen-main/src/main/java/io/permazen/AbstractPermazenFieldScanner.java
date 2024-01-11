
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeToken;

import io.permazen.annotation.PermazenTransient;
import io.permazen.annotation.PermazenType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.regex.Pattern;

/**
 * Support superclass for field annotation scanners.
 */
abstract class AbstractPermazenFieldScanner<T, A extends Annotation> extends AnnotationScanner<T, A> {

    protected final PermazenType permazenType;

    AbstractPermazenFieldScanner(PermazenClass<T> pclass, Class<A> annotationType, PermazenType permazenType) {
        super(pclass, annotationType);
        this.permazenType = permazenType;
    }

    protected abstract A getDefaultAnnotation();

    @Override
    protected A getAnnotation(Method method) {

        // Look for existing annotation
        final A annotation = super.getAnnotation(method);
        if (annotation != null)
            return annotation;

        // Check for auto-generated bean property getter method, but only if no overridden annotated method exists
        if (!this.hasAnnotatedOverriddenMethod(method.getDeclaringClass(), method.getName(), method.getParameterTypes())
          && this.isAutoPropertyCandidate(method))
            return this.getDefaultAnnotation();

        // Skip this method
        return null;
    }

    protected boolean hasAnnotatedOverriddenMethod(Class<?> klass, String name, Class<?>[] parameterTypes) {
        if (this.hasAnnotatedMethod(klass, name, parameterTypes))
            return true;
        for (Class<?> iface : klass.getInterfaces()) {
            if (this.hasAnnotatedOverriddenMethod(iface, name, parameterTypes))
                return true;
        }
        final Class<?> superclass = klass.getSuperclass();
        return superclass != null && this.hasAnnotatedOverriddenMethod(superclass, name, parameterTypes);
    }

    private boolean hasAnnotatedMethod(Class<?> klass, String name, Class<?>[] parameterTypes) {
        try {
            return klass.getMethod(name, parameterTypes).isAnnotationPresent(this.annotationType);
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    protected boolean isAutoPropertyCandidate(Method method) {
        if (!this.permazenType.autogenFields())
            return false;
        if ((method.getModifiers() & Modifier.STATIC) != 0)
            return false;
        if (this.hasJTransientAnnotation(method))
            return false;
        if (!this.permazenType.autogenNonAbstract() && this.isOverriddenByConcreteMethod(method))
            return false;
        if ((method.getModifiers() & (Modifier.PROTECTED | Modifier.PUBLIC)) == 0)
            return false;
        if ((method.getModifiers() & Modifier.PRIVATE) != 0)
            return false;
        if (!Pattern.compile("(is|get)(.+)").matcher(method.getName()).matches())
            return false;
        if (method.getParameterTypes().length != 0)
            return false;
        if (method.getReturnType() == Void.TYPE)
            return false;
        return true;
    }

    private boolean isOverriddenByConcreteMethod(Method method) {
        if ((method.getModifiers() & Modifier.ABSTRACT) == 0)                   // quick check
            return true;
        final Class<?> methodType = method.getDeclaringClass();
        for (TypeToken<?> typeToken : TypeToken.of(this.pclass.type).getTypes()) {
            final Class<?> type = typeToken.getRawType();
            if (!methodType.isAssignableFrom(type) || type.equals(methodType))  // i.e., type > methodType
                continue;
            final Method otherMethod;
            try {
                otherMethod = type.getDeclaredMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException e) {
                continue;
            }
            if ((otherMethod.getModifiers() & Modifier.ABSTRACT) == 0)
                return true;
        }
        return false;
    }

    private boolean hasJTransientAnnotation(Method method) {
        final String name = method.getName();
        final Class<?>[] ptypes = method.getParameterTypes();
        for (TypeToken<?> typeToken : TypeToken.of(method.getDeclaringClass()).getTypes()) {
            final Method override;
            try {
                override = typeToken.getRawType().getDeclaredMethod(name, ptypes);
            } catch (NoSuchMethodException e) {
                continue;
            }
            if (Util.getAnnotation(override, PermazenTransient.class) != null)
                return true;
        }
        return false;
    }
}
