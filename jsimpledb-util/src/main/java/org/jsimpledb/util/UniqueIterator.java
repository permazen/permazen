
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;

/**
 * {@link Iterator} implementation that provides a read-only view of an inner {@link Iterator}
 * in which consecutive duplicate values are eliminated.
 */
public class UniqueIterator<E> implements Iterator<E> {

    private final PeekingIterator<E> iterator;
    private final Comparator<? super E> comparator;

    /**
     * Constructor. Object equality using {@link Object#equals} is used to detect duplicates.
     *
     * @param iterator wrapped {@link Iterator}
     */
    public UniqueIterator(Iterator<? extends E> iterator) {
        this.iterator = Iterators.peekingIterator(iterator);
        this.comparator = null;
    }

    /**
     * Constructor. A zero result from the provided {@link Comparator} is used to detect duplicates.
     *
     * @param iterator wrapped {@link Iterator}
     * @param comparator used to compare consecutive values
     * @throws IllegalArgumentException if {@code comparator} is null
     */
    public UniqueIterator(Iterator<? extends E> iterator, Comparator<? super E> comparator) {
        Preconditions.checkArgument(comparator != null, "null comparator");
        this.iterator = Iterators.peekingIterator(iterator);
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
            if (this.comparator != null ? this.comparator.compare(next, peek) != 0 :
              next != null ? !next.equals(peek) : peek != null)
                break;
            this.iterator.next();
        }
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

