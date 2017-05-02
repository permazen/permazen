
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * Support superclass for {@link NavigableSet} implementations for which calculating {@link #size size()} requires
 * an iteration through all of the set's elements to count them.
 *
 * <p>
 * For a read-only implementation, subclasses should implement {@link #comparator comparator()}, {@link #contains contains()},
 * {@link #iterator iterator()}, and {@link #createSubSet createSubSet()} to handle reversed and restricted range sub-sets.
 *
 * <p>
 * For a mutable implementation, subclasses should also implement {@link #add add()}, {@link #remove remove()},
 * {@link #clear clear()}, and make the {@link #iterator iterator()} mutable.
 *
 * <p>
 * All overridden methods must be aware of the {@linkplain #bounds range restriction bounds}, if any.
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
        this(new Bounds<>());
    }

    /**
     * Primary constructor.
     *
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected AbstractNavigableSet(Bounds<E> bounds) {
        Preconditions.checkArgument(bounds != null, "null bounds");
        this.bounds = bounds;
    }

    /**
     * Removes the given element from this set if it is present.
     *
     * <p>
     * The implementation in {@link AbstractNavigableSet} always throws {@link UnsupportedOperationException}.
     */
    @Override
    public boolean remove(Object elem) {
        throw new UnsupportedOperationException("read-only set");
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
        return this.searchBelow(elem, false);
    }

    @Override
    public E floor(E elem) {
        return this.searchBelow(elem, true);
    }

    @Override
    public E ceiling(E elem) {
        return this.searchAbove(elem, true);
    }

    @Override
    public E higher(E elem) {
        return this.searchAbove(elem, false);
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
            throw new IllegalArgumentException("upper bound " + newMaxElement + " is out of bounds: " + this.bounds);
        return this.createSubSet(false, newBounds);
    }

    @Override
    public NavigableSet<E> tailSet(E newMinElement, boolean inclusive) {
        final Bounds<E> newBounds = this.bounds.withLowerBound(newMinElement, BoundType.of(inclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("lower bound " + newMinElement + " is out of bounds: " + this.bounds);
        return this.createSubSet(false, newBounds);
    }

    @Override
    public NavigableSet<E> subSet(E newMinElement, boolean minInclusive, E newMaxElement, boolean maxInclusive) {
        final Bounds<E> newBounds = new Bounds<>(newMinElement,
          BoundType.of(minInclusive), newMaxElement, BoundType.of(maxInclusive));
        if (!this.bounds.isWithinBounds(this.comparator(), newBounds))
            throw new IllegalArgumentException("new bound(s) " + newBounds + " are out of bounds: " + this.bounds);
        return this.createSubSet(false, newBounds);
    }

    @Override
    public Spliterator<E> spliterator() {
        return new Spliterators.AbstractSpliterator<E>(Long.MAX_VALUE,
          Spliterator.ORDERED | Spliterator.SORTED | Spliterator.DISTINCT) {

            private final Iterator<E> iterator = AbstractNavigableSet.this.iterator();

            @Override
            public boolean tryAdvance(Consumer<? super E> action) {
                if (!this.iterator.hasNext())
                    return false;
                action.accept(iterator.next());
                return true;
            }

            @Override
            public Comparator<? super E> getComparator() {
                return AbstractNavigableSet.this.comparator();
            }
        };
    }

    /**
     * Search for a lower element. Used to implement {@link #floor floor()} and {@link #lower lower()}.
     *
     * <p>
     * The implementation in {@link AbstractNavigableSet} checks the bounds then returns the first element from a head set.
     *
     * @param elem upper limit for search
     * @param inclusive true if {@code elem} itself is a candidate
     * @return highest element below {@code elem}, or null if not found
     */
    protected E searchBelow(E elem, boolean inclusive) {
        if (!this.isWithinLowerBound(elem))
            return null;
        final NavigableSet<E> subSet = this.isWithinUpperBound(elem) ? this.headSet(elem, inclusive) : this;
        try {
            return subSet.last();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Search for a higher element. Used to implement {@link #ceiling ceiling()} and {@link #higher higher()}.
     *
     * <p>
     * The implementation in {@link AbstractNavigableSet} checks the bounds then returns the first element from a tail set.
     *
     * @param elem lower limit for search
     * @param inclusive true if {@code elem} itself is a candidate
     * @return lowest element above {@code elem}, or null if not found
     */
    protected E searchAbove(E elem, boolean inclusive) {
        if (!this.isWithinUpperBound(elem))
            return null;
        final NavigableSet<E> subSet = this.isWithinLowerBound(elem) ? this.tailSet(elem, inclusive) : this;
        try {
            return subSet.first();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Get a non-null {@link Comparator} that sorts consistently with, and optionally reversed from, this instance.
     *
     * @param reversed whether to return a reversed {@link Comparator}
     * @return a non-null {@link Comparator}
     */
    protected Comparator<? super E> getComparator(boolean reversed) {
        return NavigableSets.getComparator(this.comparator(), reversed);
    }

    /**
     * Create a (possibly reversed) view of this instance with (possibly) tighter lower and/or upper bounds.
     * The {@code newBounds} are consistent with the new ordering (i.e., reversed relative to this instance's ordering if
     * {@code reverse} is true) and have already been range-checked against {@linkplain #bounds this instance's current bounds}.
     *
     * @param reverse whether the new set's ordering should be reversed relative to this instance's ordering
     * @param newBounds new bounds
     * @return restricted and/or reversed view of this instance
     * @throws IllegalArgumentException if {@code newBounds} is null
     * @throws IllegalArgumentException if a bound in {@code newBounds} is null and this set does not permit null elements
     */
    protected abstract NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds);

    /**
     * Determine if the given element is within this instance's lower bound (if any).
     *
     * <p>
     * The implementation in {@link AbstractNavigableSet} returns {@code this.bounds.isWithinLowerBound(this.comparator(), elem)}.
     *
     * @param elem set element
     * @return true if {@code elem} is within this instance's lower bound, or this instance has no lower bound
     */
    protected boolean isWithinLowerBound(E elem) {
        return this.bounds.isWithinLowerBound(this.comparator(), elem);
    }

    /**
     * Determine if the given element is within this instance's upper bound (if any).
     *
     * <p>
     * The implementation in {@link AbstractNavigableSet} returns {@code this.bounds.isWithinUpperBound(this.comparator(), elem)}.
     *
     * @param elem set element
     * @return true if {@code elem} is within this instance's upper bound, or this instance has no upper bound
     */
    protected boolean isWithinUpperBound(E elem) {
        return this.bounds.isWithinUpperBound(this.comparator(), elem);
    }
}

