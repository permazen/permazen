
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

/**
 * Support superclass for {@link NavigableSet} implementations that join together multiple other {@link NavigableSet}s
 * having equivalent {@link Comparator}s and for which {@link #size} is an expensive operation.
 *
 * @param <E> element type
 */
abstract class AbstractMultiNavigableSet<E> extends AbstractNavigableSet<E> {

    protected final ArrayList<? extends NavigableSet<E>> list;
    protected final Comparator<? super E> comparator;

    /**
     * Convenience constructor for the case where there are no lower or upper bounds.
     *
     * @param sets sets to be combined
     * @throws IllegalArgumentException if the {@code sets} is empty
     * @throws IllegalArgumentException if the {@link NavigableSet}s in {@code sets} do not have equal {@link Comparator}s
     */
    protected AbstractMultiNavigableSet(Iterable<? extends NavigableSet<E>> sets) {
        this(sets, AbstractMultiNavigableSet.getComparator(sets), new Bounds<E>());
    }

    /**
     * Primary constructor.
     *
     * @param sets sets to be combined
     * @param comparator common comparator
     * @param bounds range restriction
     * @throws IllegalArgumentException if the {@code sets} is empty
     * @throws IllegalArgumentException if the {@link NavigableSet}s in {@code sets} do not have equal {@link Comparator}s
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected AbstractMultiNavigableSet(Iterable<? extends NavigableSet<E>> sets,
      Comparator<? super E> comparator, Bounds<E> bounds) {
        super(bounds);
        this.list = Lists.newArrayList(sets);
        this.comparator = comparator;
    }

    @Override
    public Comparator<? super E> comparator() {
        return this.comparator;
    }

    /**
     * Get and verify a common {@link Comparator} (possibly null).
     */
    private static <E> Comparator<? super E> getComparator(Iterable<? extends NavigableSet<E>> sets) {
        final Iterator<? extends NavigableSet<E>> i = sets.iterator();
        if (!i.hasNext())
            throw new IllegalArgumentException("empty sets");
        final Comparator<? super E> comparator = i.next().comparator();
        while (i.hasNext()) {
            final Comparator<? super E> comparator2 = i.next().comparator();
            if (!(comparator == null ? comparator2 == null : comparator.equals(comparator2)))
                throw new IllegalArgumentException("sets have unequal comparators");
        }
        return comparator;
    }

    @Override
    protected final NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds) {
        final ArrayList<NavigableSet<E>> newList = new ArrayList<NavigableSet<E>>(this.list.size());
        for (NavigableSet<E> set : this.list) {
            if (newBounds.getLowerBoundType() != BoundType.NONE) {
                try {
                    set = set.tailSet(newBounds.getLowerBound(), newBounds.getLowerBoundType().isInclusive());
                } catch (IllegalArgumentException e) {
                    // ignore - lower bound is out of range
                }
            }
            if (newBounds.getUpperBoundType() != BoundType.NONE) {
                try {
                    set = set.headSet(newBounds.getUpperBound(), newBounds.getUpperBoundType().isInclusive());
                } catch (IllegalArgumentException e) {
                    // ignore - upper bound is out of range
                }
            }
            newList.add(set);
        }
        return this.createSubSet(reverse, newBounds, newList);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds.
     * The {@code newBounds} are consistent with the new ordering (i.e., reversed if {@code reverse} is true)
     * and have already been range-checked against {@linkplain #bounds this instance's current bounds}.
     *
     * @param reverse whether the new set's ordering should be reversed relative to this instance's ordering
     * @param newBounds new bounds
     * @param list list of this instance's nested sets, restricted to {@code newBounds}
     * @throws IllegalArgumentException if {@code newBounds} is null
     */
    protected abstract NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds, List<NavigableSet<E>> list);
}

