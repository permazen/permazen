
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

/**
 * Provides a read-only view of the union of two or more {@link NavigableSet}s.
 */
class UnionNavigableSet<E> extends AbstractMultiNavigableSet<E> {

    /**
     * Constructor.
     *
     * @param sets the sets to union
     */
    UnionNavigableSet(Iterable<? extends NavigableSet<E>> sets) {
        super(sets);
    }

    /**
     * Internal constructor.
     *
     * @param sets the sets to union
     * @param comparator common comparator
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected UnionNavigableSet(Iterable<? extends NavigableSet<E>> sets, Comparator<? super E> comparator, Bounds<E> bounds) {
        super(sets, comparator, bounds);
    }

    @Override
    protected NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds, List<NavigableSet<E>> newList) {
        final Comparator<? super E> newComparator = this.getComparator(reverse);
        return new UnionNavigableSet<E>(newList, newComparator, newBounds);
    }

    @Override
    public boolean contains(Object obj) {
        for (NavigableSet<E> set : this.list) {
            if (set.contains(obj))
                return true;
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        final Comparator<? super E> comparator = this.getComparator(false);
        return new UniqueIterator<E>(Iterators.mergeSorted(Lists.transform(this.list,
          new Function<NavigableSet<E>, Iterator<E>>() {
            @Override
            public Iterator<E> apply(NavigableSet<E> set) {
                return set.iterator();
            }
        }), comparator), comparator);
    }
}

