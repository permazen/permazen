
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.List;

import org.dellroad.stuff.java.EnumUtil;

/**
 * Container backed by the instances of an {@link Enum} type.
 *
 * <p>
 * Instances will auto-detect properties defined by {@link ProvidesProperty &#64;ProvidesProperty} annotations (if any),
 * and will also pre-define the following properties:
 *  <ul>
 *  <li>{@value #NAME_PROPERTY} - {@link String} property derived from {@link Enum#name Enum.name()}</li>
 *  <li>{@value #VALUE_PROPERTY} - type {@code T} property that returns the {@link Enum} instance value itself</li>
 *  <li>{@value #ORDINAL_PROPERTY} - {@link Integer} property derived from {@link Enum#ordinal Enum.ordinal()}</li>
 *  <li>{@value #TO_STRING_PROPERTY} - {@link String} property derived from {@link Enum#toString}</li>
 *  </ul>
 * </p>
 *
 * @param <T> enum type
 * @see EnumComboBox
 */
@SuppressWarnings("serial")
public class EnumContainer<T extends Enum<T>> extends SelfKeyedContainer<T> {

    public static final String NAME_PROPERTY = "name";
    public static final String VALUE_PROPERTY = "value";
    public static final String ORDINAL_PROPERTY = "ordinal";
    public static final String TO_STRING_PROPERTY = "toString";

    private static final PropertyDef<String> NAME_PROPERTY_DEF = new PropertyDef<String>(NAME_PROPERTY, String.class);
    @SuppressWarnings("rawtypes")
    private static final PropertyDef<Enum> VALUE_PROPERTY_DEF = new PropertyDef<Enum>(VALUE_PROPERTY, Enum.class);
    private static final PropertyDef<Integer> ORDINAL_PROPERTY_DEF = new PropertyDef<Integer>(ORDINAL_PROPERTY, Integer.class);
    private static final PropertyDef<String> TO_STRING_PROPERTY_DEF = new PropertyDef<String>(TO_STRING_PROPERTY, String.class);

    /**
     * Constructor.
     *
     * @param type enum type
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if {@code type} has a {@link ProvidesProperty &#64;ProvidesProperty}-annotated method
     *  with no {@linkplain ProvidesProperty#value property name specified} has a name which cannot be interpreted as a bean
     *  property "getter" method
     * @throws IllegalArgumentException if {@code type} has two {@link ProvidesProperty &#64;ProvidesProperty}-annotated
     *  fields or methods with the same {@linkplain ProvidesProperty#value property name}
     */
    public EnumContainer(final Class<T> type) {
        super(type);
        this.setProperty(NAME_PROPERTY_DEF);
        this.setProperty(VALUE_PROPERTY_DEF);
        this.setProperty(ORDINAL_PROPERTY_DEF);
        this.setProperty(TO_STRING_PROPERTY_DEF);
        this.load(this.getExposedValues(type));
    }

    /**
     * Get the {@link Enum} values to expose in this container in the desired order.
     *
     * <p>
     * The implementation in {@link EnumContainer} returns all values in their natural order.
     * Subclasses may override to filter and/or re-order.
     * </p>
     */
    protected List<T> getExposedValues(Class<T> type) {
        return EnumUtil.getValues(type);
    }

    @Override
    public <V> V getPropertyValue(T value, PropertyDef<V> propertyDef) {
        if (propertyDef.equals(VALUE_PROPERTY_DEF))
            return propertyDef.getType().cast(value);
        if (propertyDef.equals(VALUE_PROPERTY_DEF))
            return propertyDef.getType().cast(value);
        if (propertyDef.equals(ORDINAL_PROPERTY_DEF))
            return propertyDef.getType().cast(value.ordinal());
        if (propertyDef.equals(TO_STRING_PROPERTY_DEF))
            return propertyDef.getType().cast(value.toString());
        return super.getPropertyValue(value, propertyDef);
    }
}

