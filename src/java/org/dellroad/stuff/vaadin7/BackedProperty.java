
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Property;

/**
 * Extension of the {@link Property} interface for implementations that are backed by an underlying Java object.
 *
 * @param <T> the type of the underlying Java object
 * @param <V> the type of the property
 */
@SuppressWarnings("serial")
public interface BackedProperty<T, V> extends Property<V> {

    /**
     * Retrieve the underlying Java object.
     *
     * @return underlying Java object, never null
     */
    T getObject();
}

