
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.collect.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * An singleton {@link NavigableSet} implementation.
 */
class SingletonNavigableSet<E> extends AbstractNavigableSet<E> {

    private final Comparator<? super E> comparator;
    private final E value;

    /**
     * Primary constructor.
     *
     * @param comparator comparator, or null for natural ordering
     * @param value singleton value (possibly null)
     * @throws IllegalArgumentException if {@code comparator} is null and either {@code value} is null or
     *  {@code value}'s class has no natural ordering (i.e., does not implement {@link Comparable})
     */
    SingletonNavigableSet(Comparator<? super E> comparator, E value) {
        this(comparator, value, new Bounds<>());
    }

    /**
     * Internal constructor.
     *
     * @param comparator comparator, or null for natural ordering
     * @param value singleton value (possibly null)
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code comparator} is null and either {@code value} is null or
     *  {@code value}'s class has no natural ordering (i.e., does not implement {@link Comparable})
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected SingletonNavigableSet(Comparator<? super E> comparator, E value, Bounds<E> bounds) {
        super(bounds);
        this.comparator = comparator;
        if (comparator == null && !(value instanceof Comparable)) {
            throw new IllegalArgumentException("no Comparator provided but value type "
              + (value != null ? value.getClass().getName() : "null") + " does not implement Comparable");
        }
        this.value = value;
    }

    @Override
    public Comparator<? super E> comparator() {
        return this.comparator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object obj) {
        if (this.value == null)
            return obj == null;
        try {
            return this.comparator().compare(this.value, (E)obj) == 0;
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.singletonIterator(this.value);
    }

    @Override
    protected NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds) {

        // Get comparator
        final Comparator<? super E> newComparator = this.getComparator(reverse);

        // Check bounds
        return newBounds.isWithinBounds(newComparator, this.value) ?
          new SingletonNavigableSet<>(newComparator, this.value, newBounds) :
          new EmptyNavigableSet<>(newComparator, newBounds);
    }
}

