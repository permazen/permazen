
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;
import com.google.common.collect.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;

/**
 * Provides a transformed view of a wrapped {@link NavigableSet} using a strictly invertable {@link Converter}.
 *
 * @param <E> element type of this set
 * @param <W> element type of the wrapped set
 */
public class ConvertedNavigableSet<E, W> extends AbstractNavigableSet<E> {

    private final NavigableSet<W> set;
    private final Converter<E, W> elementConverter;

    /**
     * Constructor.
     *
     * @param set wrapped set
     * @param elementConverter element converter
     * @throws IllegalArgumentException if either parameter is null
     */
    public ConvertedNavigableSet(NavigableSet<W> set, Converter<E, W> elementConverter) {
        this(set, elementConverter, new Bounds<E>());
    }

    /**
     * Internal constructor.
     *
     * @param set wrapped set
     * @param elementConverter element converter
     * @throws IllegalArgumentException if any parameter is null
     */
    ConvertedNavigableSet(NavigableSet<W> set, Converter<E, W> elementConverter, Bounds<E> bounds) {
        super(bounds);
        if (set == null)
            throw new IllegalArgumentException("null set");
        if (elementConverter == null)
            throw new IllegalArgumentException("null elementConverter");
        this.set = set;
        this.elementConverter = elementConverter;
    }

    public Converter<E, W> getConverter() {
        return this.elementConverter;
    }

    @Override
    public Comparator<? super E> comparator() {
        return new ConvertedComparator<E, W>(this.set.comparator(), this.elementConverter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object obj) {
        W wobj = null;
        if (obj != null) {
            try {
                wobj = this.elementConverter.convert((E)obj);
            } catch (ClassCastException e) {
                return false;
            }
        }
        return this.set.contains(wobj);
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.transform(this.set.iterator(), this.elementConverter.reverse());
    }

    @Override
    public boolean add(E obj) {
        return this.set.add(obj != null ? this.elementConverter.convert(obj) : null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object obj) {
        W wobj = null;
        if (obj != null) {
            try {
                wobj = this.elementConverter.convert((E)obj);
            } catch (ClassCastException e) {
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
        final W wlower = newBounds.getLowerBoundType() != BoundType.NONE && lower != null ?
          this.elementConverter.convert(lower) : null;
        final W wupper = newBounds.getUpperBoundType() != BoundType.NONE && upper != null ?
          this.elementConverter.convert(upper) : null;
        NavigableSet<W> subSet = reverse ? this.set.descendingSet() : this.set;
        if (newBounds.getLowerBoundType() != BoundType.NONE && newBounds.getUpperBoundType() != BoundType.NONE) {
            subSet = subSet.subSet(
              wlower, newBounds.getLowerBoundType().isInclusive(),
              wupper, newBounds.getUpperBoundType().isInclusive());
        } else if (newBounds.getLowerBoundType() != BoundType.NONE)
            subSet = subSet.tailSet(wlower, newBounds.getLowerBoundType().isInclusive());
        else if (newBounds.getUpperBoundType() != BoundType.NONE)
            subSet = subSet.headSet(wupper, newBounds.getUpperBoundType().isInclusive());
        return new ConvertedNavigableSet<E, W>(subSet, this.elementConverter, newBounds);
    }
}

