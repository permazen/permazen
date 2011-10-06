
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.data.Item;

/**
 * Defines a Vaadin property, having a name, which is also the property ID, and its type.
 */
public final class PropertyDef<T> {

    private final String name;
    private final Class<T> type;

    public PropertyDef(String name, Class<T> type) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.name = name;
        this.type = type;
    }

    /**
     * Get the name of this property.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the type of the property value that this instance represents.
     */
    public Class<T> getType() {
        return this.type;
    }

    /**
     * Read the property that this instance represents from the given {@link Item}.
     */
    public T read(Item item) {
        return this.type.cast(item.getItemProperty(this.name).getValue());
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.type.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PropertyDef))
            return false;
        PropertyDef<?> that = (PropertyDef<?>)obj;
        return this.name.equals(that.name) && this.type == that.type;
    }
}

