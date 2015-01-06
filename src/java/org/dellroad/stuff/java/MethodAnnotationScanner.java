
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
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Scan a class hierarchy for annotated methods in an override-aware manner.
 *
 * <p>
 * Instances find all methods annotated with a specific annotation in a given Java class or any of its superclasses
 * and superinterfaces, while also being override-aware, i.e., filtering out annotations on overridden supertype methods
 * when the overriding method also has the annotation. This operation is performed by {@link #findAnnotatedMethods}.
 * </p>
 *
 * <p>
 * Subclasses may validate the annotations, and control which annotated methods to include and/or reject, by overriding
 * {@link #includeMethod includeMethod()}.
 * </p>
 *
 * @param <T> Java type to be introspected
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
     * Build set of annotated methods, with overridden annotated methods omitted.
     *
     * @return the set of all annotated methods, with overridden annotated methods removed
     *  when the overriding method is also annotated
     */
    public Set<MethodInfo> findAnnotatedMethods() {

        // Find all methods
        final HashSet<MethodInfo> set = new HashSet<MethodInfo>();
        this.findMethodInfos(this.type, set);

        // Remove overridden and duplicate methods
        final LinkedHashMap<String, HashSet<MethodInfo>> nameSetMap = new LinkedHashMap<String, HashSet<MethodInfo>>();
    infoLoop:
        for (MethodInfo methodInfo : set) {
            final String name = methodInfo.getMethod().getName();
            final Method method = methodInfo.getMethod();
            HashSet<MethodInfo> nameSet = nameSetMap.get(name);
            if (nameSet == null) {
                nameSet = new HashSet<MethodInfo>();
                nameSetMap.put(name, nameSet);
                nameSet.add(methodInfo);
                continue;
            }
            for (Iterator<MethodInfo> i = nameSet.iterator(); i.hasNext(); ) {
                final Method otherMethod = i.next().getMethod();
                if (MethodAnnotationScanner.overrides(method, otherMethod)) {
                    i.remove();
                    continue;
                }
                if (MethodAnnotationScanner.overrides(otherMethod, method))
                    continue infoLoop;
            }
            nameSet.add(methodInfo);
        }

        // Return result
        final HashSet<MethodInfo> result = new HashSet<MethodInfo>();
        for (HashSet<MethodInfo> nameSet : nameSetMap.values())
            result.addAll(nameSet);
        return result;
    }

    /**
     * Scan the given type and all its supertypes for annotated methods and add them to the list.
     *
     * @param klass type to scan, possibly null
     * @param set set to add methods to
     */
    protected void findMethodInfos(Class<?> klass, Set<MethodInfo> set) {

        // Stop at null
        if (klass == null)
            return;

        // Search methods
        for (Method method : klass.getDeclaredMethods()) {
            final A annotation = this.getAnnotation(method);
            if (annotation == null || !this.includeMethod(method, annotation))
                continue;
            final MethodInfo methodInfo = this.createMethodInfo(method, annotation);
            if (methodInfo != null)
                set.add(methodInfo);
        }

        // Recurse on interfaces
        for (Class<?> iface : klass.getInterfaces())
            this.findMethodInfos(iface, set);

        // Recurse on superclass
        this.findMethodInfos(klass.getSuperclass(), set);
    }

    /**
     * Get the annotation on the given method.
     *
     * <p>
     * The implementation in {@link MethodAnnotationScanner} just invokes {@code method.getAnnotation()}.
     * Subclasses may override to automatically synthesize annotations, etc.
     * </p>
     *
     * @param method method in question
     * @return annotation for {@code method}, or null if there is none
     */
    protected A getAnnotation(Method method) {
        return method.getAnnotation(this.annotationType);
    }

    /**
     * Determine whether the annotated method should be included.
     *
     * <p>
     * The implementation in {@link MethodAnnotationScanner} returns true if {@code method} takes zero parameters
     * and returns anything other than void, otherwise false.
     * </p>
     *
     * <p>
     * Subclasses may apply different tests and optionally throw an exception if a method is improperly annotated.
     * </p>
     *
     * @param method method to check
     * @param annotation method's annotation
     * @throws RuntimeException if method is erroneously annotated
     * @return true to include method, false to ignore it
     */
    protected boolean includeMethod(Method method, A annotation) {
        return method.getReturnType() != void.class && method.getParameterTypes().length == 0;
    }

    /**
     * Determine if one method strictly overrides another.
     *
     * @param override possible overriding method (i.e., subclass method)
     * @param original possible overriding method (i.e., superclass method)
     * @return true if {@code override} overrides {@code original}, otherwise false;
     *  if {@code override} equals {@code original}, false is returned
     */
    public static boolean overrides(Method override, Method original) {

        // Check class hierarchy
        if (!original.getDeclaringClass().isAssignableFrom(override.getDeclaringClass())
          || original.getDeclaringClass() == override.getDeclaringClass())
            return false;

        // Compare names
        if (!original.getName().equals(override.getName()))
            return false;

        // Check staticness
        if ((original.getModifiers() & Modifier.STATIC) != 0 || (override.getModifiers() & Modifier.STATIC) != 0)
            return false;

        // Compare (raw) parameter types
        if (!Arrays.equals(original.getParameterTypes(), override.getParameterTypes()))
            return false;

        // Done
        return true;
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
     *
     * @see MethodAnnotationScanner#findAnnotatedMethods
     */
    public class MethodInfo {

        private final Method method;
        private final A annotation;

        public MethodInfo(Method method, A annotation) {
            if (method == null)
                throw new IllegalArgumentException("null method");
            if (annotation == null)
                throw new IllegalArgumentException("null annotation");
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
         * Invoke the method and return the result. Any checked exception thrown is rethrown after being wrapped
         * in a {@link RuntimeException}.
         *
         * @return result of invoking method
         * @throws RuntimeException if invocation fails
         */
        public Object invoke(T obj, Object... params) {
            try {
                return this.method.invoke(obj, params);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException)
                    throw (RuntimeException)e.getCause();
                if (e.getCause() instanceof Error)
                    throw (Error)e.getCause();
                throw new RuntimeException(e.getCause());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final MethodAnnotationScanner<?, ?>.MethodInfo that = (MethodAnnotationScanner<?, ?>.MethodInfo)obj;
            return this.method.equals(that.method) && this.annotation.equals(that.annotation);
        }

        @Override
        public int hashCode() {
            return this.method.hashCode() ^ this.annotation.hashCode();
        }
    }
}

