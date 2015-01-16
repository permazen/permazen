
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.regex.Pattern;

import org.jsimpledb.util.AnnotationScanner;

/**
 * Support superclass for field annotation scanners.
 */
abstract class AbstractFieldScanner<T, A extends Annotation> extends AnnotationScanner<T, A> {

    private final boolean autogenFields;

    AbstractFieldScanner(JClass<T> jclass, Class<A> annotationType, boolean autogenFields) {
        super(jclass, annotationType);
        this.autogenFields = autogenFields;
    }

    protected abstract A getDefaultAnnotation();

    @Override
    protected A getAnnotation(Method method) {

        // Look for existing annotation
        final A annotation = super.getAnnotation(method);
        if (annotation != null)
            return annotation;

        // Check for auto-generated bean property getter method
        if (this.isAutoPropertyCandidate(method))
            return this.getDefaultAnnotation();

        // Skip this method
        return null;
    }

    protected boolean isAutoPropertyCandidate(Method method) {
        if (!this.autogenFields)
            return false;
        if (this.isOverriddenByConcreteMethod(method))
            return false;
        if ((method.getModifiers() & (Modifier.ABSTRACT | Modifier.STATIC)) != Modifier.ABSTRACT)
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
        final Class<?> methodType = method.getDeclaringClass();
        for (Class<?> type = this.jclass.type;
          type != null && methodType.isAssignableFrom(type) && !type.equals(methodType);
          type = type.getSuperclass()) {
            final Method otherMethod;
            try {
                otherMethod = type.getDeclaredMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException e) {
                continue;
            }
            if (otherMethod != null && (otherMethod.getModifiers() & Modifier.ABSTRACT) == 0)
                return true;
        }
        return false;
    }
}

