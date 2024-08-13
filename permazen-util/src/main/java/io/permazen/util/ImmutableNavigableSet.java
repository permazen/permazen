
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
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An immutable {@link NavigableSet} implementation optimized for read efficiency.
 *
 * <p>
 * Because the elements are stored in an array, it's also possible to get the element by index; see {@link #get get()}.
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

// Constructors

    /**
     * Constructor.
     *
     * @param source data source
     * @throws IllegalArgumentException if {@code source} is null
     */
    public ImmutableNavigableSet(NavigableSet<E> source) {
        this(source, checkNull(source).comparator());
    }

    /**
     * Constructor.
     *
     * @param source data source
     * @param comparator element comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code source} is null
     */
    @SuppressWarnings("unchecked")
    public ImmutableNavigableSet(NavigableSet<E> source, Comparator<? super E> comparator) {
        this((E[])checkNull(source).toArray(), comparator);
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to: {@code ImmutableNavigableSet(elems, 0, elems.length, comparator)}.
     *
     * @param elems sorted element array; <i>this array is not copied and must be already sorted</i>
     * @param comparator element comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code elems} is null
     */
    public ImmutableNavigableSet(E[] elems, Comparator<? super E> comparator) {
        this(new Bounds<>(), checkNull(elems), 0, elems.length, comparator);
    }

    /**
     * Primary constructor.
     *
     * @param elems sorted element array; <i>this array is not copied and must be already sorted</i>
     * @param minIndex minimum index into array (inclusive)
     * @param maxIndex maximum index into array (exclusive)
     * @param comparator element comparator, or null for natural ordering
     * @throws IllegalArgumentException if {@code elems} is null
     * @throws IllegalArgumentException if {@code elems} has length less than {@code maxIndex}
     * @throws IllegalArgumentException if {@code minIndex > maxIndex}
     */
    public ImmutableNavigableSet(E[] elems, int minIndex, int maxIndex, Comparator<? super E> comparator) {
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
        this.actualComparator = NavigableSets.comparatorOrNatural(this.comparator);
        for (int i = minIndex + 1; i < maxIndex; i++)
            assert this.actualComparator.compare(this.elems[i - 1], this.elems[i]) < 0;
    }

// Extra Methods

    /**
     * Get the element at the specified index.
     *
     * @param index index into the ordered element array
     * @return the element at the specified index
     * @throws IndexOutOfBoundsException if {@code index} is negative or greater than or equal to {@link #size}
     */
    public E get(int index) {
        Objects.checkIndex(index, this.size());
        return this.elems[this.minIndex + index];
    }

    /**
     * Search for the given element in the underlying array.
     *
     * <p>
     * This method works like {@link Arrays#binarySearch(Object[], Object) Arrays.binarySearch()}, returning
     * either the index of {@code elem} in the underlying array given to the constructor if found, or else
     * the one's complement of {@code elem}'s insertion point.
     *
     * <p>
     * The array searched is the array given to the constructor, or if {@link #ImmutableNavigableSet(NavigableSet)}
     * was used, an array containing all of the elements in this set.
     *
     * @param elem element to search for
     * @return index of {@code elem}, or {@code -(insertion point) - 1} if not found
     */
    public int binarySearch(E elem) {
        return this.find(elem);
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
    public CloseableIterator<E> iterator() {
        return new Iter(this.minIndex, this.maxIndex, 1);
    }

    @Override
    public CloseableIterator<E> descendingIterator() {
        return new Iter(this.maxIndex - 1, this.minIndex - 1, -1);
    }

    @Override
    public Spliterator<E> spliterator() {
        return Spliterators.spliterator(this.elems, this.minIndex, this.maxIndex,
          Spliterator.CONCURRENT | Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.SORTED);
    }

    @Override
    public Stream<E> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    @Override
    protected Spliterator<E> buildSpliterator(Iterator<E> iterator) {
        throw new UnsupportedOperationException();
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

    private static <T> T checkNull(T obj) {
        if (obj == null)
            throw new IllegalArgumentException();
        return obj;
    }

// Iter

    private class Iter implements CloseableIterator<E> {

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

        @Override
        public void close() {
        }
    }
}
