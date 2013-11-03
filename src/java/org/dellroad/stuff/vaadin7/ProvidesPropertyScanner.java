
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.dellroad.stuff.java.MethodAnnotationScanner;
import org.dellroad.stuff.java.Primitive;

/**
 * Scans a Java class hierarchy for {@link ProvidesProperty &#64;ProvidesProperty} and
 * {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated getter methods and creates
 * a corresponding set of {@link PropertyDef} property definitions and a {@link SortingPropertyExtractor}
 * that will extract the properties from instances of the given class and sort them accordingly.
 *
 * @param <T> Java class to be introspected
 * @see ProvidesProperty &#64;ProvidesProperty
 * @see ProvidesPropertySort &#64;ProvidesPropertySort
 */
public class ProvidesPropertyScanner<T> {

    private final ArrayList<PropertyDef<?>> propertyList = new ArrayList<PropertyDef<?>>();
    private final SortingPropertyExtractor<T> propertyExtractor;

    /**
     * Constructor.
     *
     * @param type Java class to be introspected
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if an annotated method with no {@linkplain ProvidesProperty#value property name specified}
     *  has a name which cannot be interpreted as a bean property "getter" method
     * @throws IllegalArgumentException if {@code type} has two {@link ProvidesProperty &#64;ProvidesProperty}-annotated
     *  fields or methods with the same {@linkplain ProvidesProperty#value property name}
     */
    public ProvidesPropertyScanner(Class<T> type) {

        // Sanity check
        if (type == null)
            throw new IllegalArgumentException("null type");

        // Scan for @ProvidesProperty and @ProvidesPropertySort annotations
        final List<MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo> providesPropertyMethods
          = new MethodAnnotationScanner<T, ProvidesProperty>(type, ProvidesProperty.class).buildMethodInfoList();
        final List<MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo> providesPropertySortMethods
          = new MethodAnnotationScanner<T, ProvidesPropertySort>(type, ProvidesPropertySort.class).buildMethodInfoList();

        // Check for duplicate @ProvidesProperty names
        final HashMap<String, MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo> providesPropertyNameMap
          = new HashMap<String, MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo>();
        for (MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo methodInfo : providesPropertyMethods) {
            final String propertyName = this.getPropertyName(methodInfo);
            final MethodAnnotationScanner<T, ?>.MethodInfo previous = providesPropertyNameMap.put(propertyName, methodInfo);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate @" + ProvidesProperty.class.getSimpleName()
                  + " declaration for property `" + propertyName + "' on method " + previous.getMethod()
                  + " and " + methodInfo.getMethod());
            }
        }

        // Check for duplicate @ProvidesPropertySort names, etc.
        final HashMap<String, MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo> providesPropertySortNameMap
          = new HashMap<String, MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo>();
        for (MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo methodInfo : providesPropertySortMethods) {
            final String propertyName = this.getSortPropertyName(methodInfo);
            final MethodAnnotationScanner<T, ?>.MethodInfo previous = providesPropertySortNameMap.put(propertyName, methodInfo);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate @" + ProvidesPropertySort.class.getSimpleName()
                  + " declaration for property `" + propertyName + "' on method " + previous.getMethod()
                  + " and " + methodInfo.getMethod());
            }
            final Class<?> methodType = methodInfo.getMethod().getReturnType();
            if (!Comparable.class.isAssignableFrom(methodType)) {
                throw new IllegalArgumentException("invalid @" + ProvidesPropertySort.class.getSimpleName()
                  + " declaration for property `" + propertyName + "': method " + methodInfo.getMethod()
                  + " return type " + methodType.getName() + " is not an instance of " + Comparable.class.getName());
            }
        }

        // Build PropertyDef list
        for (MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo methodInfo : providesPropertyMethods) {
            final String propertyName = this.getPropertyName(methodInfo);

            // Get property type
            Class<?> propertyType = methodInfo.getMethod().getReturnType();

            // Get property default value
            Object defaultValue = null;
            if (propertyType.isPrimitive()) {
                final Primitive primitiveType = Primitive.get(propertyType);
                defaultValue = primitiveType.getDefaultValue();
                propertyType = primitiveType.getWrapperType();
            }

            // Get sort property (if any)
            final MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo sortMethodInfo
              = providesPropertySortNameMap.get(propertyName);

            // Create property definition
            this.propertyList.add(this.createAnnotationPropertyDef(
              propertyName, propertyType, defaultValue, methodInfo, sortMethodInfo));
        }

        // Build PropertyExtractor
        this.propertyExtractor = new SortingPropertyExtractor<T>() {

            @Override
            @SuppressWarnings("unchecked")
            public <V> V getPropertyValue(T obj, PropertyDef<V> propertyDef) {
                if (!(propertyDef instanceof AnnotationPropertyDef))
                    throw new RuntimeException("unknown property " + propertyDef);
                final AnnotationPropertyDef<V> annotationPropertyDef = (AnnotationPropertyDef<V>)propertyDef;
                return propertyDef.getType().cast(annotationPropertyDef.getMethodInfo().readValue(obj));
            }

            @Override
            @SuppressWarnings("unchecked")
            public boolean canSort(PropertyDef<?> propertyDef) {
                if (!(propertyDef instanceof AnnotationPropertyDef))
                    return false;
                final AnnotationPropertyDef<?> annotationPropertyDef = (AnnotationPropertyDef)propertyDef;
                return annotationPropertyDef.getSortMethodInfo() != null;
            }

            @Override
            @SuppressWarnings("unchecked")
            public int sort(PropertyDef<?> propertyDef, T obj1, T obj2) {
                if (!(propertyDef instanceof AnnotationPropertyDef))
                    throw new UnsupportedOperationException("unknown property " + propertyDef);
                final AnnotationPropertyDef<?> annotationPropertyDef = (AnnotationPropertyDef)propertyDef;
                final MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo sortMethodInfo
                  = annotationPropertyDef.getSortMethodInfo();
                if (sortMethodInfo == null)
                    throw new UnsupportedOperationException("can't sort property " + propertyDef);
                final Comparable<Object> value1 = (Comparable<Object>)annotationPropertyDef.getSortMethodInfo().readValue(obj1);
                final Comparable<Object> value2 = (Comparable<Object>)annotationPropertyDef.getSortMethodInfo().readValue(obj2);
                if (value1 == null && value2 != null)
                    return -1;
                if (value1 != null && value2 == null)
                    return 1;
                if (value1 == null && value2 == null)
                    return 0;
                return value1.compareTo(value2);
            }
        };
    }

    /**
     * Get the list of {@link PropertyDef}s generated from the annotated methods.
     *
     * <p>
     * All of the properties in the returned list can be extracted from instances of this reader's configured
     * class by the {@link PropertyExtractor} returned by {@link #getPropertyExtractor}.
     * </p>
     *
     * @return unmodifiable list of properties
     * @see #getPropertyExtractor
     */
    public List<PropertyDef<?>> getPropertyDefs() {
        return Collections.unmodifiableList(this.propertyList);
    }

    /**
     * Get the {@link PropertyExtractor} that extracts Vaadin {@link com.vaadin.data.Property} values
     * from instances of the annotated class when given one of the {@link PropertyDef}s returned by
     * {@link #getPropertyDefs}.
     *
     * @see #getPropertyDefs
     */
    public SortingPropertyExtractor<T> getPropertyExtractor() {
        return this.propertyExtractor;
    }

    private String getPropertyName(MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo methodInfo) {
        return methodInfo.getAnnotation().value().length() > 0 ?
          methodInfo.getAnnotation().value() : methodInfo.getMethodPropertyName();
    }

    private String getSortPropertyName(MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo methodInfo) {
        return methodInfo.getAnnotation().value().length() > 0 ?
          methodInfo.getAnnotation().value() : methodInfo.getMethodPropertyName();
    }

    // This method exists solely to bind the generic type
    private <V> AnnotationPropertyDef<V> createAnnotationPropertyDef(String propertyName, Class<V> propertyType,
      Object defaultValue, MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo methodInfo,
      MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo sortMethodInfo) {
        return new AnnotationPropertyDef<V>(propertyName, propertyType,
          propertyType.cast(defaultValue), methodInfo, sortMethodInfo);
    }

    private class AnnotationPropertyDef<V> extends PropertyDef<V> {

        private final MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo methodInfo;
        private final MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo sortMethodInfo;

        AnnotationPropertyDef(String name, Class<V> type, V defaultValue,
          MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo methodInfo,
          MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo sortMethodInfo) {
            super(name, type, defaultValue);
            this.methodInfo = methodInfo;
            this.sortMethodInfo = sortMethodInfo;
        }

        public MethodAnnotationScanner<T, ProvidesProperty>.MethodInfo getMethodInfo() {
            return this.methodInfo;
        }

        public MethodAnnotationScanner<T, ProvidesPropertySort>.MethodInfo getSortMethodInfo() {
            return this.sortMethodInfo;
        }
    };
}

