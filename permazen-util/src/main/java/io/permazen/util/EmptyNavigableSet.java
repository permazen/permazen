
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.Comparator;

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
    EmptyNavigableSet(Comparator<? super E> comparator) {
        this(comparator, new Bounds<>());
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
    public CloseableIterator<E> iterator() {
        return CloseableIterator.emptyIterator();
    }

    @Override
    protected EmptyNavigableSet<E> createSubSet(boolean reverse, Bounds<E> bounds) {
        return new EmptyNavigableSet<>(this.getComparator(reverse), bounds);
    }
}
