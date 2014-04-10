
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;

/**
 * Support superclass for {@link NavigableSet} implementations for which calculating {@link #size size()} requires
 * an iteration through all of the set's elements to count them.
 *
 * <p>
 * For a read-only implementation, subclasses should implement {@link #comparator comparator()}, {@link #contains contains()},
 * {@link #iterator iterator()}, and {@link #createSubSet createSubSet()} to handle reversed and restricted range sub-sets.
 * </p>
 *
 * <p>
 * For a mutable implementation, subclasses should also implement {@link #add add()}, {@link #remove remove()},
 * {@link #clear clear()}, and make the {@link #iterator iterator()} mutable.
 * </p>
 *
 * <p>
 * All overridden methods must be aware of the {@linkplain #bounds range restriction bounds}, if any.
 * </p>
 *
 * @param <E> element type
 */
public abstract class AbstractNavigableSet<E> extends AbstractIterationSet<E> implements NavigableSet<E> {

    /**
     * Element range bounds associated with this instance.
     */
    protected final Bounds<E> bounds;

    /**
     * Convenience constructor for the case where there are no lower or upper bounds.
     */
    protected AbstractNavigableSet() {
        this(new Bounds<E>());
    }

    /**
     * Primary constructor.
     *
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected AbstractNavigableSet(Bounds<E> bounds) {
        if (bounds == null)
            throw new IllegalArgumentException("null bounds");
        this.bounds = bounds;
    }

    @Override
    public E first() {
        return this.iterator().next();
    }

    @Override
    public E last() {
        return this.descendingIterator().next();
    }

    @Override
    public E pollFirst() {
        final Iterator<E> i = this.iterator();
        if (!i.hasNext())
            return null;
        final E value = i.next();
        i.remove();
        return value;
    }

    @Override
    public E pollLast() {
        return this.descendingSet().pollFirst();
    }

    @Override
    public Iterator<E> descendingIterator() {
        return this.descendingSet().iterator();
    }

    @Override
    public E lower(E elem) {
        if (!this.bounds.isWithinLowerBound(this.comparator(), elem))
            return null;
        final NavigableSet<E> subSet = this.bounds.isWithinUpperBound(this.comparator(), elem) ? this.headSet(elem, false) : this;
        try {
            return subSet.last();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public E floor(E elem) {
        if (!this.bounds.isWithinLowerBound(this.comparator(), elem))
            return null;
        final NavigableSet<E> subSet = this.bounds.isWithinUpperBound(this.comparator(), elem) ? this.headSet(elem, true) : this;
        try {
            return subSet.last();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public E ceiling(E elem) {
        if (!this.bounds.isWithinUpperBound(this.comparator(), elem))
            return null;
        final NavigableSet<E> subSet = this.bounds.isWithinLowerBound(this.comparator(), elem) ? this.tailSet(elem, true) : this;
        try {
            return subSet.first();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public E higher(E elem) {
        if (!this.bounds.isWithinUpperBound(this.comparator(), elem))
            return null;
        final NavigableSet<E> subSet = this.bounds.isWithinLowerBound(this.comparator(), elem) ? this.tailSet(elem, false) : this;
        try {
            return subSet.first();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public NavigableSet<E> headSet(E newMaxElement) {
        return this.headSet(newMaxElement, false);
    }

    @Override
    public NavigableSet<E> tailSet(E newMinElement) {
        return this.tailSet(newMinElement, true);
    }

    @Override
    public NavigableSet<E> subSet(E newMinElement, E newMaxElement) {
        return this.subSet(newMinElement, true, newMaxElement, false);
    }

    @Override
    public NavigableSet<E> descendingSet() {
        return this.createSubSet(true, this.bounds.reverse());
    }

    @Override
    public NavigableSet<E> headSet(E newMaxElement, boolean inclusive) {
        final Bounds<E> newBounds = this.bounds.withUpperBound(newMaxElement, BoundType.of(inclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("upper bound is out of range");
        return this.createSubSet(false, newBounds);
    }

    @Override
    public NavigableSet<E> tailSet(E newMinElement, boolean inclusive) {
        final Bounds<E> newBounds = this.bounds.withLowerBound(newMinElement, BoundType.of(inclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("lower bound is out of range");
        return this.createSubSet(false, newBounds);
    }

    @Override
    public NavigableSet<E> subSet(E newMinElement, boolean minInclusive, E newMaxElement, boolean maxInclusive) {
        final Bounds<E> newBounds = new Bounds<>(newMinElement,
          BoundType.of(minInclusive), newMaxElement, BoundType.of(maxInclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("bound(s) are out of range");
        return this.createSubSet(false, newBounds);
    }

    /**
     * Get an {@link Comparator} that sorts consistently with (and optionally reversed from) this instance.
     *
     * @param reversed whether to return a reversed {@link Comparator}
     * @return a non-null {@link Comparator}
     */
    protected Comparator<? super E> getComparator(boolean reversed) {
        final Comparator<? super E> comparator = this.comparator();
        if (comparator != null)
            return AbstractNavigableSet.possiblyReverse(comparator, reversed);
        return reversed ? Collections.<E>reverseOrder() : new NaturalComparator();
    }

    // This method exists solely to bind the generic type parameters
    private static <T> Comparator<T> possiblyReverse(Comparator<T> comparator, boolean reverse) {
        return reverse ? Collections.reverseOrder(comparator) : comparator;
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds.
     * The {@code newBounds} are consistent with the new ordering (i.e., reversed relative to this instance's ordering if
     * {@code reverse} is true) and have already been range-checked against {@linkplain #bounds this instance's current bounds}.
     *
     * @param reverse whether the new set's ordering should be reversed relative to this instance's ordering
     * @param newBounds new bounds
     * @throws IllegalArgumentException if {@code newBounds} is null
     * @throws IllegalArgumentException if a bound in {@code newBounds} is null and this set does not permit null elements
     */
    protected abstract NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds);

// NaturalComparator

    private class NaturalComparator implements Comparator<E> {

        @Override
        @SuppressWarnings("unchecked")
        public int compare(E e1, E e2) {
            return ((Comparable<E>)e1).compareTo(e2);
        }
    }
}

