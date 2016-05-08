
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

/**
 * Provides a read-only view of the difference of two or more {@link NavigableSet}s.
 */
class DifferenceNavigableSet<E> extends AbstractMultiNavigableSet<E> {

    /**
     * Constructor.
     *
     * @param sets the sets to difference
     */
    @SuppressWarnings("unchecked")
    DifferenceNavigableSet(NavigableSet<E> set1, NavigableSet<E> set2) {
        super(Lists.newArrayList(set1, set2));
    }

    /**
     * Internal constructor.
     *
     * @param sets the sets to difference
     * @param comparator common comparator
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    @SuppressWarnings("unchecked")
    protected DifferenceNavigableSet(NavigableSet<E> set1, NavigableSet<E> set2,
      Comparator<? super E> comparator, Bounds<E> bounds) {
        super(Lists.newArrayList(set1, set2), comparator, bounds);
    }

    @Override
    protected NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds, List<NavigableSet<E>> newList) {
        final Comparator<? super E> newComparator = this.getComparator(reverse);
        return new DifferenceNavigableSet<E>(newList.get(0), newList.get(1), newComparator, newBounds);
    }

    @Override
    public boolean contains(Object obj) {
        return this.list.get(0).contains(obj) && !this.list.get(1).contains(obj);
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.filter(this.list.get(0).iterator(), Predicates.not(Predicates.in(this.list.get(1))));
    }
}

