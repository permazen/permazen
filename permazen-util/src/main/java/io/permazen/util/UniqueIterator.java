
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

/**
 * {@link CloseableIterator} implementation that provides a read-only view of an inner {@link Iterator}
 * in which consecutive duplicate values are eliminated.
 */
public class UniqueIterator<E> implements CloseableIterator<E> {

    private final Iterator<? extends E> inner;
    private final PeekingIterator<E> iterator;
    private final Comparator<? super E> comparator;

    /**
     * Constructor.
     *
     * <p>
     * Object equality using {@link Object#equals} is used to detect duplicates.
     *
     * @param iterator wrapped {@link Iterator}
     * @throws IllegalArgumentException if {@code iterator} is null
     */
    public UniqueIterator(Iterator<? extends E> iterator) {
        this(iterator, null);
    }

    /**
     * Constructor.
     *
     * <p>
     * A zero result from {@code comparator} is used to detect duplicates.
     * If {@code comparator} is null, {@link Object#equals} is used.
     *
     * @param iterator wrapped {@link Iterator}
     * @param comparator used to compare consecutive values, or null to use {@link Object#equals}
     * @throws IllegalArgumentException if {@code iterator} is null
     */
    public UniqueIterator(Iterator<? extends E> iterator, Comparator<? super E> comparator) {
        Preconditions.checkArgument(iterator != null, "null iterator");
        Preconditions.checkArgument(comparator != null, "null comparator");
        this.inner = iterator;
        this.iterator = Iterators.<E>peekingIterator(this.inner);
        this.comparator = comparator;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public E next() {
        final E next = this.iterator.next();
        while (this.iterator.hasNext()) {
            final E peek = this.iterator.peek();
            final boolean duplicate = this.comparator != null ?
              this.comparator.compare(next, peek) == 0 : Objects.equals(next, peek);
            if (!duplicate)
                break;
            this.iterator.next();
        }
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (this.inner instanceof CloseableIterator)
            ((CloseableIterator<? extends E>)this.inner).close();
    }
}
