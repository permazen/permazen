
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

/**
 * A {@link PropertyDef} representing the {@link String} value of an object using {@link Object#toString Object#toString()}.
 *
 * <p>
 * Instances also serve as a {@link SortingPropertyExtractor} that can actually extract the property from any object.
 * </p>
 */
public class StringValuePropertyDef extends PropertyDef<String> implements SortingPropertyExtractor<Object> {

    /**
     * Constructor.
     *
     * @param name property name; also serves as the property ID
     */
    public StringValuePropertyDef(String name) {
        super(name, String.class, null);
    }

// PropertyExtractor

    @Override
    @SuppressWarnings("unchecked")
    public <V> V getPropertyValue(Object obj, PropertyDef<V> propertyDef) {
        if (!(propertyDef instanceof StringValuePropertyDef))
            throw new IllegalArgumentException("unknown property " + propertyDef);
        return (V)obj.toString();
    }

// SortingPropertyExtractor

    @Override
    public boolean canSort(PropertyDef<?> propertyDef) {
        return propertyDef instanceof StringValuePropertyDef;
    }

    @Override
    public int sort(PropertyDef<?> propertyDef, Object obj1, Object obj2) {
        if (!(propertyDef instanceof StringValuePropertyDef))
            throw new UnsupportedOperationException("unknown property " + propertyDef);
        return obj1.toString().compareTo(obj2.toString());
    }
}

