
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.beans.Introspector;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Scans for annotated methods in a class hierarchy.
 * Both superclasses and superinterfaces are introspected, and overrides are automatically detected.
 * Only methods taking zero parameters and returning non-void are detected.
 *
 * @param <T> Java class to be introspected
 * @param <A> Java annotation type
 */
public class MethodAnnotationScanner<T, A extends Annotation> {

    protected final Class<T> type;
    protected final Class<A> annotationType;

    /**
     * Constructor.
     *
     * @param type Java class to be introspected
     * @param annotationType Java annotation type to search for
     * @throws IllegalArgumentException if either parameter is null
     */
    public MethodAnnotationScanner(Class<T> type, Class<A> annotationType) {

        // Sanity check
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (annotationType == null)
            throw new IllegalArgumentException("null annotationType");
        this.type = type;
        this.annotationType = annotationType;
    }

    /**
     * Build list of annotated methods, with overridden annotated methods omitted.
     *
     * @return list of annotated methods with any overridden annotated methods removed
     */
    public List<MethodInfo> buildMethodInfoList() {

        // Find all methods
        final ArrayList<MethodInfo> list = new ArrayList<MethodInfo>();
        this.findMethodInfos(this.type, list);

        // Sort them
        Collections.sort(list);

        // Remove overridden methods
        final LinkedHashMap<String, MethodInfo> map = new LinkedHashMap<String, MethodInfo>();
        for (MethodInfo methodInfo : list) {
            if (map.containsKey(methodInfo.getMethod().getName()))
                continue;
            map.put(methodInfo.getMethod().getName(), methodInfo);
        }

        // Return result
        return new ArrayList<MethodInfo>(map.values());
    }

    /**
     * Scan the given type and all its supertypes for annotated methods and add them to the list
     *
     * @param klass type to scan, possibly null
     * @param list list to append to
     */
    protected void findMethodInfos(Class<?> klass, List<MethodInfo> list) {

        // Stop at null
        if (klass == null)
            return;

        // Search methods
        for (Method method : klass.getDeclaredMethods()) {
            if (method.getReturnType() == void.class || method.getParameterTypes().length > 0)
                continue;
            final A annotation = method.getAnnotation(this.annotationType);
            if (annotation != null) {
                final MethodInfo methodInfo = this.createMethodInfo(method, annotation);
                if (methodInfo != null)
                    list.add(methodInfo);
            }
        }

        // Recurse on interfaces
        for (Class<?> iface : klass.getInterfaces())
            this.findMethodInfos(iface, list);

        // Recurse on superclass
        this.findMethodInfos(klass.getSuperclass(), list);
    }

    /**
     * Create a new {@link MethodInfo} instance corresponding to the given annotated method.
     *
     * <p>
     * The implementation in {@link MethodAnnotationScanner} just instantiates a {@link MethodInfo} directly.
     * </p>
     *
     * @param method the method
     * @param annotation the annotation annotating the method
     */
    protected MethodInfo createMethodInfo(Method method, A annotation) {
        return new MethodInfo(method, annotation);
    }

    /**
     * Holds information about an annotated method detected by a {@link MethodAnnotationScanner}.
     */
    public class MethodInfo implements Comparable<MethodInfo> {

        private final Method method;
        private final A annotation;

        public MethodInfo(Method method, A annotation) {
            this.method = method;
            this.annotation = annotation;
            try {
                this.method.setAccessible(true);
            } catch (SecurityException e) {
                // ignore
            }
        }

        /**
         * Get the annotated method.
         *
         * @return annotated method taking zero parameters and returning non-void
         */
        public Method getMethod() {
            return this.method;
        }

        /**
         * Get the method annotattion.
         */
        public A getAnnotation() {
            return this.annotation;
        }

        /**
         * Get the Java bean property name implied by this method's name, if any.
         *
         * @throws IllegalArgumentException if the method's name does not follow Java bean conventions
         */
        public String getMethodPropertyName() {
            final String name = this.method.getName();
            if (name.startsWith("get") && name.length() > 3)
                return Introspector.decapitalize(name.substring(3));
            if (name.startsWith("is") && name.length() > 2 && Primitive.get(method.getReturnType()) == Primitive.BOOLEAN)
                return Introspector.decapitalize(name.substring(2));
            throw new IllegalArgumentException("can't infer property name from non-Java bean method " + this.method);
        }

        /**
         * Invoke the method to read its value.
         *
         * @return result of invoking method
         */
        public Object readValue(T obj) {
            try {
                return this.method.invoke(obj);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Sorts instances first by declaring class (subtypes before supertypes), then by method name.
         */
        @Override
        public int compareTo(MethodInfo that) {
            final Class<?> thisClass = this.method.getDeclaringClass();
            final Class<?> thatClass = that.method.getDeclaringClass();
            if (thisClass != thatClass) {
                if (thatClass.isAssignableFrom(thisClass))
                    return -1;
                if (thisClass.isAssignableFrom(thatClass))
                    return 1;
            }
            return this.method.getName().compareTo(that.method.getName());
        }
    }
}

