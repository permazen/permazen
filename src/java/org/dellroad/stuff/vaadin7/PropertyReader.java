
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.beans.Introspector;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.dellroad.stuff.java.Primitive;

/**
 * Builds a {@link PropertyExtractor} and a list of {@link PropertyDef}s from a class having
 * {@link ProvidesProperty &#64;ProvidesProperty}-annotated methods by introspecting the class
 * and its superclasses and superinterfaces.
 *
 * <p>
 * See {@link ProvidesProperty &#64;ProvidesProperty} for further details.
 * </p>
 *
 * @param <T> Java class to be introspected
 * @see ProvidesProperty &#64;ProvidesProperty
 */
public class PropertyReader<T> {

    private final ArrayList<PropertyInfo<?>> propertyList = new ArrayList<PropertyInfo<?>>();

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
    public PropertyReader(Class<T> type) {

        // Sanity check
        if (type == null)
            throw new IllegalArgumentException("null type");

        // Introspect fields and methods
        this.findProperties(type);

        // Sort by name - note, sort is stable so subclass methods will appear prior to method(s) they override
        Collections.sort(this.propertyList);

        // Check for duplicate names, but allow method overrides to declare the same property
        PropertyInfo<?> previousInfo = null;
        for (Iterator<PropertyInfo<?>> i = this.propertyList.iterator(); i.hasNext(); ) {
            final PropertyInfo<?> propertyInfo = i.next();

            // Check for duplicate name
            if (previousInfo != null && propertyInfo.getName().equals(previousInfo.getName())) {

                // Allow method overrides to declare the same property name
                final AccessibleObject previousMember = previousInfo.getMember();
                final AccessibleObject propertyMember = propertyInfo.getMember();
                if (previousMember instanceof Method && propertyMember instanceof Method) {
                    i.remove();
                    continue;
                }

                // Otherwise, there is a duplicate property name
                throw new IllegalArgumentException("duplicate property name `" + propertyInfo.getName()
                  + "' (on " + previousMember + " and " + propertyMember + ")");
            }
            previousInfo = propertyInfo;
        }

        // Trim array
        this.propertyList.trimToSize();
    }

    private void findProperties(Class<?> klass) {

        // Stop at null
        if (klass == null)
            return;

        // Search fields
        for (Field field : klass.getDeclaredFields()) {
            final ProvidesProperty annotation = field.getAnnotation(ProvidesProperty.class);
            if (annotation != null)
                this.addPropertyInfo(field, annotation.value(), field.getType());
        }

        // Search methods
        for (Method method : klass.getDeclaredMethods()) {
            if (method.getReturnType() == void.class || method.getParameterTypes().length > 0)
                continue;
            final ProvidesProperty annotation = method.getAnnotation(ProvidesProperty.class);
            if (annotation != null)
                this.addPropertyInfo(method, annotation.value(), method.getReturnType());
        }

        // Recurse on interfaces
        for (Class<?> iface : klass.getInterfaces())
            this.findProperties(iface);

        // Recurse on superclass
        this.findProperties(klass.getSuperclass());
    }

    private void addPropertyInfo(AccessibleObject member, String name, Class<?> type) {

        // Sanity check type
        if (type == void.class)
            throw new IllegalArgumentException("property `" + name + "' has void type on " + member);

        // Get (non-primitive) type and default value
        final Primitive primitiveType = Primitive.get(type);
        Object defaultValue = null;
        if (type.isPrimitive()) {
            defaultValue = primitiveType.getDefaultValue();
            type = primitiveType.getWrapperType();
        }

        // Get name, if not explicitly specified
        if (name.length() == 0) {
            if (member instanceof Field)
                name = ((Field)member).getName();
            else {
                name = ((Method)member).getName();
                if (name.startsWith("get") && name.length() > 3)
                    name = Introspector.decapitalize(name.substring(3));
                else if (name.startsWith("is") && name.length() > 2 && primitiveType == Primitive.BOOLEAN)
                    name = Introspector.decapitalize(name.substring(2));
                else
                    throw new IllegalArgumentException("can't infer property name from non-getter method " + member);
            }
        }

        // Add new property to list
        this.propertyList.add(this.createPropertyInfo(member, name, type, defaultValue));
    }

    // This method exists solely to bind the generic type
    private <V> PropertyInfo<V> createPropertyInfo(AccessibleObject member, String name, Class<V> type, Object defaultValue) {
        return new PropertyInfo<V>(member, name, type, type.cast(defaultValue));
    }

    /**
     * Get a {@link PropertyExtractor} that extracts Vaadin {@link com.vaadin.data.Property} values
     * from instances of this reader's configured class.
     */
    public PropertyExtractor<T> getPropertyExtractor() {
        return new PropertyExtractor<T>() {
            @Override
            public <V> V getPropertyValue(T obj, PropertyDef<V> propertyDef) {
                for (PropertyInfo<?> propertyInfo : PropertyReader.this.propertyList) {
                    if (propertyDef.equals(propertyInfo.getPropertyDef()))
                        return propertyDef.getType().cast(propertyInfo.readValue(obj));
                }
                throw new RuntimeException("unknown property " + propertyDef);
            }
        };
    }

    /**
     * Get the list of {@link PropertyDef}s derived from the {@link ProvidesProperty &#64;ProvidesProperty}-annotated
     * fields and methods of this instance's configured class.
     *
     * <p>
     * All of the properties in the returned list can be extracted from instances of this reader's configured
     * class by the {@link PropertyExtractor} returned by {@link #getPropertyExtractor}.
     * </p>
     */
    public List<PropertyDef<?>> getPropertyDefs() {
        final ArrayList<PropertyDef<?>> result = new ArrayList<PropertyDef<?>>(this.propertyList.size());
        for (PropertyInfo<?> propertyInfo : this.propertyList)
            result.add(propertyInfo.getPropertyDef());
        return result;
    }

    private class PropertyInfo<V> implements Comparable<PropertyInfo<?>> {

        private final AccessibleObject member;
        private final PropertyDef<V> propertyDef;

        PropertyInfo(AccessibleObject member, String name, Class<V> type, V defaultValue) {
            this.propertyDef = new PropertyDef<V>(name, type, defaultValue);
            this.member = member;
            try {
                this.member.setAccessible(true);
            } catch (SecurityException e) {
                // ignore
            }
        }

        String getName() {
            return this.propertyDef.getName();
        }

        AccessibleObject getMember() {
            return this.member;
        }

        PropertyDef<V> getPropertyDef() {
            return this.propertyDef;
        }

        Object readValue(T obj) {
            if (this.member instanceof Field) {
                final Field field = (Field)this.member;
                try {
                    return field.get(obj);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            } else {
                final Method method = (Method)this.member;
                try {
                    return method.invoke(obj);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public int compareTo(PropertyInfo<?> that) {
            return this.getName().compareTo(that.getName());
        }
    }
}

