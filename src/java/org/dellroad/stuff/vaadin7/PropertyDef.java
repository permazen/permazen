
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Container;
import com.vaadin.data.Item;
import com.vaadin.data.Property;

/**
 * Vaadin 7 version of {@link org.dellroad.stuff.vaadin.PropertyDef}.
 *
 * @param <T> property's value type
 * @see org.dellroad.stuff.vaadin.PropertyDef
 */
public class PropertyDef<T> extends org.dellroad.stuff.vaadin.PropertyDef<T> {

    /**
     * Convenience contructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  PropertyDef(name, type, null);
     *  </pre></blockquote>
     * </p>
     */
    public PropertyDef(String name, Class<T> type) {
        super(name, type);
    }

    /**
     * Primary constructor.
     *
     * @param name property name; also serves as the property ID
     * @param type property type
     * @param defaultValue default value for the property; may be null
     */
    public PropertyDef(String name, Class<T> type, T defaultValue) {
        super(name, type, defaultValue);
    }

    @Override
    public Property<T> get(Item item) {
        return this.cast(item.getItemProperty(this.getPropertyId()));
    }

    @Override
    public Property<T> get(Container container, Object itemId) {
        return this.cast(container.getContainerProperty(itemId, this.getPropertyId()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Property<T> cast(Property/*<?>*/ property) {
        return (Property<T>)super.cast(property);
    }
}

