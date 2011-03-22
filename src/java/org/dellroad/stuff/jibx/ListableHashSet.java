
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.util.Collection;
import java.util.LinkedHashSet;

import org.jibx.runtime.JiBXException;

/**
 * {@link java.util.Set} implementation with these properties which make it suitable for use with JiBX:
 * <ul>
 * <li>Iteration order reflects addition order (this property is inherited from {@link LinkedHashSet}</li>
 * <li>An {@link #addUnique} method that throws {@link JiBXException} if the item is already in the set
 *  (suitable for use as a JiBX {@code add-method})</li>
 * </ul>
 *
 * @since 1.0.64
 */
public class ListableHashSet<E> extends LinkedHashSet<E> {

    public ListableHashSet() {
    }

    public ListableHashSet(Collection<? extends E> c) {
        super(c);
    }

    public ListableHashSet(int initialCapacity) {
        super(initialCapacity);
    }

    public ListableHashSet(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Add an item to a set while verifying that the item is not already in the set.
     *
     * @throws JiBXException if item is already in the set
     */
    public void addUnique(E item) throws JiBXException {
        if (this.contains(item))
            throw new JiBXException("duplicate item in set: " + item);
        this.add(item);
    }
}

