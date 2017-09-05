
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;

/**
 * An immutable {@link NavigableSet} implementation optimized for read efficiency.
 *
 * @param <E> element type
 */
@SuppressWarnings("serial")
public class ImmutableNavigableSet<E> extends AbstractNavigableSet<E> {

    private final E[] elems;
    private final int minIndex;
    private final int maxIndex;
    private final Comparator<? super E> comparator;
    private final Comparator<? super E> actualComparator;

    /**
     * Constructor.
     *
     * @param source data source
     * @throws IllegalArgumentException if {@code source} is null
     */
    @SuppressWarnings("unchecked")
    public ImmutableNavigableSet(NavigableSet<E> source) {
        this((E[])source.toArray(), source.comparator());
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to: {@code ImmutableNavigableSet(elems, 0, elems.length, comparator)}.
     *
     * @param elems sorted element array
     * @param comparator element comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code elems} is null
     */
    public ImmutableNavigableSet(E[] elems, Comparator<? super E> comparator) {
        this(new Bounds<>(), elems, 0, elems.length, comparator);
    }

    /**
     * Primary constructor.
     *
     * @param elems sorted element array
     * @param minIndex minimum index into array (inclusive)
     * @param maxIndex maximum index into array (exclusive)
     * @param comparator element comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code elems} is null
     * @throws IllegalArgumentException if {@code elems} has length less than {@code maxIndex}
     * @throws IllegalArgumentException if {@code minIndex > maxIndex}
     */
    ImmutableNavigableSet(E[] elems, int minIndex, int maxIndex, Comparator<? super E> comparator) {
        this(new Bounds<>(), elems, minIndex, maxIndex, comparator);
    }

    @SuppressWarnings("unchecked")
    ImmutableNavigableSet(Bounds<E> bounds, E[] elems, int minIndex, int maxIndex, Comparator<? super E> comparator) {
        super(bounds);
        Preconditions.checkArgument(elems != null);
        Preconditions.checkArgument(minIndex >= 0 && maxIndex >= minIndex);
        Preconditions.checkArgument(elems.length >= maxIndex);
        this.elems = elems;
        this.minIndex = minIndex;
        this.maxIndex = maxIndex;
        this.comparator = comparator;
        this.actualComparator = this.comparator != null ? this.comparator : (Comparator<E>)Comparator.naturalOrder();
        for (int i = minIndex + 1; i < maxIndex; i++)
            assert this.actualComparator.compare(this.elems[i - 1], this.elems[i]) < 0;
    }

// NavigableSet

    @Override
    public Comparator<? super E> comparator() {
        return this.comparator;
    }

    @Override
    public boolean isEmpty() {
        return this.minIndex == this.maxIndex;
    }

    @Override
    public int size() {
        return this.maxIndex - this.minIndex;
    }

    @Override
    public boolean contains(Object obj) {
        return this.find(obj) >= 0;
    }

    @Override
    public E first() {
        return this.elems[this.checkIndex(this.minIndex)];
    }

    @Override
    public E last() {
        return this.elems[this.checkIndex(this.maxIndex - 1)];
    }

    @Override
    public E pollFirst() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E pollLast() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E lower(E elem) {
        return this.findElem(elem, -1, -1);
    }

    @Override
    public E floor(E elem) {
        return this.findElem(elem, -1, 0);
    }

    @Override
    public E higher(E elem) {
        return this.findElem(elem, 0, 1);
    }

    @Override
    public E ceiling(E elem) {
        return this.findElem(elem, 0, 0);
    }

    @Override
    public Iterator<E> iterator() {
        return new Iter(this.minIndex, this.maxIndex, 1);
    }

    @Override
    public Iterator<E> descendingIterator() {
        return new Iter(this.maxIndex - 1, this.minIndex - 1, -1);
    }

    @Override
    public Spliterator<E> spliterator() {
        return Spliterators.spliterator(this.elems, this.minIndex, this.maxIndex,
          Spliterator.CONCURRENT | Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.SORTED);
    }

    @Override
    protected ImmutableNavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds) {

        // Get upper and lower bounds; note: "newBounds" are already reversed
        final E minBound = reverse ? newBounds.getUpperBound() : newBounds.getLowerBound();
        final E maxBound = reverse ? newBounds.getLowerBound() : newBounds.getUpperBound();
        final BoundType minBoundType = reverse ? newBounds.getUpperBoundType() : newBounds.getLowerBoundType();
        final BoundType maxBoundType = reverse ? newBounds.getLowerBoundType() : newBounds.getUpperBoundType();

        // Calculate the index range in our current array corresponding to the new bounds
        final int newMinIndex;
        switch (minBoundType) {
        case INCLUSIVE:
            newMinIndex = this.findNearby(minBound, 0, 0);
            break;
        case EXCLUSIVE:
            newMinIndex = this.findNearby(minBound, 0, 1);
            break;
        case NONE:
            newMinIndex = this.minIndex;
            break;
        default:
            throw new RuntimeException("internal error");
        }
        final int newMaxIndex;
        switch (maxBoundType) {
        case INCLUSIVE:
            newMaxIndex = this.findNearby(maxBound, 0, 1);
            break;
        case EXCLUSIVE:
            newMaxIndex = this.findNearby(maxBound, 0, 0);
            break;
        case NONE:
            newMaxIndex = this.maxIndex;
            break;
        default:
            throw new RuntimeException("internal error");
        }

        // Create new instance
        if (reverse) {
            final int newSize = newMaxIndex - newMinIndex;
            return new ImmutableNavigableSet<E>(newBounds,
              ImmutableNavigableSet.reverseArray(Arrays.copyOfRange(this.elems, newMinIndex, newMaxIndex)),
              0, newSize, ImmutableNavigableSet.reversedComparator(this.comparator));
        } else
            return new ImmutableNavigableSet<E>(newBounds, this.elems, newMinIndex, newMaxIndex, this.comparator);
    }

    static <T> T[] reverseArray(T[] array) {
        int i = 0;
        int j = array.length - 1;
        while (i < j) {
            T temp = array[i];
            array[i] = array[j];
            array[j] = temp;
            i++;
            j--;
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    static <T> Comparator<T> reversedComparator(Comparator<T> comparator) {
        return comparator == null ? (Comparator<T>)Comparator.reverseOrder() :
          comparator.equals(Comparator.reverseOrder()) ? null : comparator.reversed();
    }

    private E findElem(Object elem, int notFoundOffset, int foundOffset) {
        final int index = this.findNearby(elem, notFoundOffset, foundOffset);
        if (index < this.minIndex || index >= this.maxIndex)
            return null;
        return this.elems[index];
    }

    private int findNearby(Object elem, int notFoundOffset, int foundOffset) {
        final int index = this.find(elem);
        return index < 0 ? ~index + notFoundOffset : index + foundOffset;
    }

    @SuppressWarnings("unchecked")
    private int find(Object elem) {
        return Arrays.binarySearch(this.elems, this.minIndex, this.maxIndex, (E)elem, this.actualComparator);
    }

    private int checkIndex(final int index) {
        if (index < this.minIndex || index >= this.maxIndex)
            throw new NoSuchElementException();
        return index;
    }

// Iter

    private class Iter implements Iterator<E> {

        private final int stopIndex;
        private final int step;

        private int index;

        Iter(int startIndex, int stopIndex, int step) {
            this.index = startIndex;
            this.stopIndex = stopIndex;
            this.step = step;
        }

        @Override
        public boolean hasNext() {
            return this.index != this.stopIndex;
        }

        @Override
        public E next() {
            if (this.index == this.stopIndex)
                throw new NoSuchElementException();
            final E elem = ImmutableNavigableSet.this.elems[this.index];
            this.index += this.step;
            return elem;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

