
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * An empty {@link java.util.NavigableSet} implementation.
 */
class EmptyNavigableSet<E> extends AbstractNavigableSet<E> {

    private final Comparator<? super E> comparator;

    /**
     * Primary constructor.
     *
     * @param comparator comparator, or null for natural ordering
     */
    public EmptyNavigableSet(Comparator<? super E> comparator) {
        this(comparator, new Bounds<E>());
    }

    /**
     * Internal constructor.
     *
     * @param comparator comparator, or null for natural ordering
     */
    protected EmptyNavigableSet(Comparator<? super E> comparator, Bounds<E> bounds) {
        super(bounds);
        this.comparator = comparator;
    }

    @Override
    public Comparator<? super E> comparator() {
        return this.comparator;
    }

    @Override
    public boolean contains(Object obj) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Iterator<E> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    protected EmptyNavigableSet<E> createSubSet(boolean reverse, Bounds<E> bounds) {
        return new EmptyNavigableSet<E>(this.getComparator(reverse), bounds);
    }
}

