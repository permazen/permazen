
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Scans for annotated methods in a class hierarchy.
 * Both superclasses and superinterfaces are introspected, and overrides are automatically detected.
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

        // Remove overridden methods
        final LinkedHashMap<String, HashSet<MethodInfo>> map = new LinkedHashMap<String, HashSet<MethodInfo>>();
    infoLoop:
        for (MethodInfo methodInfo : list) {
            final String name = methodInfo.getMethod().getName();
            HashSet<MethodInfo> set = map.get(name);
            if (set == null) {
                set = new HashSet<MethodInfo>();
                map.put(name, set);
                set.add(methodInfo);
                continue;
            }
            for (Iterator<MethodInfo> i = set.iterator(); i.hasNext(); ) {
                final MethodInfo otherInfo = i.next();
                if (MethodAnnotationScanner.overrides(methodInfo.getMethod(), otherInfo.getMethod())) {
                    i.remove();
                    continue;
                }
                if (MethodAnnotationScanner.overrides(otherInfo.getMethod(), methodInfo.getMethod()))
                    continue infoLoop;
            }
            set.add(methodInfo);
        }

        // Return result
        final ArrayList<MethodInfo> result = new ArrayList<MethodInfo>();
        for (HashSet<MethodInfo> set : map.values())
            result.addAll(set);
        return result;
    }

    /**
     * Scan the given type and all its supertypes for annotated methods and add them to the list.
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
            final A annotation = method.getAnnotation(this.annotationType);
            if (annotation == null || !this.includeMethod(method, annotation))
                continue;
            final MethodInfo methodInfo = this.createMethodInfo(method, annotation);
            if (methodInfo != null)
                list.add(methodInfo);
        }

        // Recurse on interfaces
        for (Class<?> iface : klass.getInterfaces())
            this.findMethodInfos(iface, list);

        // Recurse on superclass
        this.findMethodInfos(klass.getSuperclass(), list);
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
     * Determine if a method overrides another.
     *
     * @param override possible overriding method (i.e., subclass method)
     * @param original possible overriding method (i.e., superclass method)
     * @return true if {@code override} overrides {@code original}, otherwise false;
     *  if both are the same method, false is returned
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
     */
    public class MethodInfo {

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
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

