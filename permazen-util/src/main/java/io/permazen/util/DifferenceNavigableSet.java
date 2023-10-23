
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

/**
 * Provides a read-only view of the difference of two {@link NavigableSet}s.
 */
class DifferenceNavigableSet<E> extends AbstractMultiNavigableSet<E> {

    /**
     * Constructor.
     *
     * @param set1 first set
     * @param set2 second set
     */
    @SuppressWarnings("unchecked")
    DifferenceNavigableSet(NavigableSet<E> set1, NavigableSet<E> set2) {
        super(Lists.newArrayList(set1, set2));
    }

    /**
     * Internal constructor.
     *
     * @param set1 first set
     * @param set2 second set
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
        return new DifferenceNavigableSet<>(newList.get(0), newList.get(1), newComparator, newBounds);
    }

    @Override
    public boolean contains(Object obj) {
        return this.list.get(0).contains(obj) && !this.list.get(1).contains(obj);
    }

    @Override
    public CloseableIterator<E> iterator() {
        return new Iter<>(this.list.get(0), this.list.get(1));
    }

// Iter

    private static class Iter<E> extends AbstractIterator<E> implements CloseableIterator<E> {

        private final Iterator<E> iter1;
        private final NavigableSet<E> set2;

        Iter(NavigableSet<E> set1, NavigableSet<E> set2) {
            this.iter1 = set1.iterator();
            this.set2 = set2;
        }

        @Override
        protected E computeNext() {
            while (this.iter1.hasNext()) {
                final E next = this.iter1.next();
                if (!this.set2.contains(next))
                    return next;
            }
            return this.endOfData();
        }

        @Override
        public void close() {
            if (this.iter1 instanceof CloseableIterator)
                ((CloseableIterator<E>)this.iter1).close();
        }
    }
}
