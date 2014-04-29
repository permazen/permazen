
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Item;

/**
 * Extension of the {@link Item} interface for implementations that are backed by an underlying Java object.
 *
 * @param <T> the type of the underlying Java object
 */
@SuppressWarnings("serial")
public interface BackedItem<T> extends Item {

    /**
     * Retrieve the underlying Java object.
     *
     * @return underlying Java object, never null
     */
    T getObject();
}

