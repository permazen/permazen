
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Provides a transformed view of a wrapped {@link Set} using a strictly invertable {@link Converter}.
 *
 * @param <E> element type of this set
 * @param <W> element type of the wrapped set
 */
public class ConvertedSet<E, W> extends AbstractIterationSet<E> {

    private final Set<W> set;
    private final Converter<E, W> converter;

    /**
     * Constructor.
     *
     * @param set wrapped set
     * @param converter element converter
     * @throws IllegalArgumentException if any parameter is null
     */
    public ConvertedSet(Set<W> set, Converter<E, W> converter) {
        Preconditions.checkArgument(set != null, "null set");
        Preconditions.checkArgument(converter != null, "null converter");
        this.set = set;
        this.converter = converter;
    }

    /**
     * Get the wrapped {@link Set}.
     *
     * @return the wrapped {@link Set}.
     */
    public Set<W> getWrappedSet() {
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
    @SuppressWarnings("unchecked")
    public boolean contains(Object obj) {
        W wobj = null;
        if (obj != null) {
            try {
                wobj = this.converter.convert((E)obj);
            } catch (ClassCastException e) {
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
}
