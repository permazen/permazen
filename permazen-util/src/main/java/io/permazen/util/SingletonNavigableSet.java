
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Spliterator;

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
            throw new IllegalArgumentException(String.format(
              "no Comparator provided but %s does not implement Comparable",
              value != null ? "value type " + value.getClass().getName() : "null value"));
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
            return NavigableSets.comparatorOrNatural(this.comparator()).compare(this.value, (E)obj) == 0;
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public CloseableIterator<E> iterator() {
        return CloseableIterator.wrap(Collections.singleton(this.value).iterator());
    }

    @Override
    protected Spliterator<E> buildSpliterator(Iterator<E> iterator) {
        return Collections.singleton(this.value).spliterator();
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
