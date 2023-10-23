
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.stream.Stream;

/**
 * Provides a transformed view of a wrapped {@link NavigableSet} using a strictly invertable {@link Converter}.
 *
 * <p>
 * Supplied {@link Converter}s may throw {@link ClassCastException} or {@link IllegalArgumentException}
 * if given an objects whose runtime type does not match the expected type.
 *
 * @param <E> element type of this set
 * @param <W> element type of the wrapped set
 */
public class ConvertedNavigableSet<E, W> extends AbstractNavigableSet<E> {

    private final NavigableSet<W> set;
    private final Converter<E, W> converter;

    /**
     * Constructor.
     *
     * @param set wrapped set
     * @param converter element converter
     * @throws IllegalArgumentException if either parameter is null
     */
    public ConvertedNavigableSet(NavigableSet<W> set, Converter<E, W> converter) {
        this(set, converter, new Bounds<>());
    }

    /**
     * Internal constructor.
     *
     * @param set wrapped set
     * @param converter element converter
     * @throws IllegalArgumentException if any parameter is null
     */
    ConvertedNavigableSet(NavigableSet<W> set, Converter<E, W> converter, Bounds<E> bounds) {
        super(bounds);
        Preconditions.checkArgument(set != null, "null set");
        Preconditions.checkArgument(converter != null, "null converter");
        this.set = set;
        this.converter = converter;
    }

    /**
     * Get the wrapped {@link NavigableSet}.
     *
     * @return the wrapped {@link NavigableSet}.
     */
    public NavigableSet<W> getWrappedNavigableSet() {
        return this.set;
    }

    /**
     * Get the {@link Converter} associated with this instance.
     *
     * @return associated {@link Converter}
     */
    public Converter<E, W> getConverter() {
        return this.converter;
    }

    @Override
    public Comparator<? super E> comparator() {
        return new ConvertedComparator<E, W>(this.set.comparator(), this.converter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object obj) {
        W wobj = null;
        if (obj != null) {
            try {
                wobj = this.converter.convert((E)obj);
            } catch (IllegalArgumentException | ClassCastException e) {
                return false;
            }
        }
        return this.set.contains(wobj);
    }

    @Override
    public CloseableIterator<E> iterator() {
        final Iterator<W> wrappedIterator = this.set.iterator();
        final Iterator<E> convertedIterator = Iterators.transform(wrappedIterator, this.converter.reverse());
        return wrappedIterator instanceof CloseableIterator ?
          CloseableIterator.wrap(convertedIterator, (CloseableIterator<W>)wrappedIterator) :
          CloseableIterator.wrap(convertedIterator);
    }

    @Override
    public Spliterator<E> spliterator() {
        return new ConvertedSpliterator<E, W>(this.set.spliterator(), this.converter.reverse());
    }

    @Override
    public Stream<E> stream() {
        return this.set.stream().map(this.converter.reverse()::convert);
    }

    @Override
    public boolean add(E obj) {
        return this.set.add(obj != null ? this.converter.convert(obj) : null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object obj) {
        W wobj = null;
        if (obj != null) {
            try {
                wobj = this.converter.convert((E)obj);
            } catch (IllegalArgumentException | ClassCastException e) {
                return false;
            }
        }
        return this.set.remove(wobj);
    }

    @Override
    public void clear() {
        this.set.clear();
    }

    @Override
    public boolean isEmpty() {
        return this.set.isEmpty();
    }

    @Override
    public int size() {
        return this.set.size();
    }

    @Override
    protected E searchBelow(E elem, boolean inclusive) {
        try {
            return super.searchBelow(elem, inclusive);
        } catch (IllegalArgumentException e) {                      // handle case where elem is out of bounds
            final E last;
            try {
                last = this.last();
            } catch (NoSuchElementException e2) {
                return null;
            }
            return this.getComparator(false).compare(elem, last) > 0 ? last : null;
        }
    }

    @Override
    protected E searchAbove(E elem, boolean inclusive) {
        try {
            return super.searchAbove(elem, inclusive);
        } catch (IllegalArgumentException e) {                      // handle case where elem is out of bounds
            final E first;
            try {
                first = this.first();
            } catch (NoSuchElementException e2) {
                return null;
            }
            return this.getComparator(false).compare(elem, first) < 0 ? first : null;
        }
    }

    @Override
    protected NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds) {
        final E lower = newBounds.getLowerBound();
        final E upper = newBounds.getUpperBound();
        final W wlower = newBounds.getLowerBoundType() != BoundType.NONE && lower != null ? this.converter.convert(lower) : null;
        final W wupper = newBounds.getUpperBoundType() != BoundType.NONE && upper != null ? this.converter.convert(upper) : null;
        NavigableSet<W> subSet = reverse ? this.set.descendingSet() : this.set;
        if (newBounds.getLowerBoundType() != BoundType.NONE && newBounds.getUpperBoundType() != BoundType.NONE) {
            subSet = subSet.subSet(
              wlower, newBounds.getLowerBoundType().isInclusive(),
              wupper, newBounds.getUpperBoundType().isInclusive());
        } else if (newBounds.getLowerBoundType() != BoundType.NONE)
            subSet = subSet.tailSet(wlower, newBounds.getLowerBoundType().isInclusive());
        else if (newBounds.getUpperBoundType() != BoundType.NONE)
            subSet = subSet.headSet(wupper, newBounds.getUpperBoundType().isInclusive());
        return new ConvertedNavigableSet<>(subSet, this.converter, newBounds);
    }
}

