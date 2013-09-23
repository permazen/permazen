
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
 *  <li>{@link #NAME_PROPERTY "name"} - {@link String} property derived from {@link Enum#name Enum.name()}</li>
 *  <li>{@link #VALUE_PROPERTY "value"} - type {@code T} property that returns the {@link Enum} instance value itself</li>
 *  <li>{@link #ORDINAL_PROPERTY "ordinal"} - {@link Integer} property derived from {@link Enum#ordinal Enum.ordinal()}</li>
 *  <li>{@link #TO_STRING_PROPERTY "toString"} - {@link String} property derived from {@link Enum#toString}</li>
 *  </ul>
 * </p>
 *
 * @param <T> enum type
 * @see EnumComboBox
 */
@SuppressWarnings("serial")
public class EnumContainer<T extends Enum<T>> extends SelfKeyedContainer<T> {

    public static final PropertyDef<String> NAME_PROPERTY = new PropertyDef<String>("name", String.class);
    @SuppressWarnings("rawtypes")
    public static final PropertyDef<Enum> VALUE_PROPERTY = new PropertyDef<Enum>("value", Enum.class);
    public static final PropertyDef<Integer> ORDINAL_PROPERTY = new PropertyDef<Integer>("ordinal", Integer.class);
    public static final PropertyDef<String> TO_STRING_PROPERTY = new PropertyDef<String>("toString", String.class);

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
        this.setProperty(NAME_PROPERTY);
        this.setProperty(VALUE_PROPERTY);
        this.setProperty(ORDINAL_PROPERTY);
        this.setProperty(TO_STRING_PROPERTY);
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
        if (propertyDef.equals(VALUE_PROPERTY))
            return propertyDef.getType().cast(value);
        if (propertyDef.equals(VALUE_PROPERTY))
            return propertyDef.getType().cast(value);
        if (propertyDef.equals(ORDINAL_PROPERTY))
            return propertyDef.getType().cast(value.ordinal());
        if (propertyDef.equals(TO_STRING_PROPERTY))
            return propertyDef.getType().cast(value.toString());
        return super.getPropertyValue(value, propertyDef);
    }
}

