
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides a transformed view of a wrapped {@link List} using a strictly invertable {@link Converter}.
 *
 * @param <E> element type of this list
 * @param <W> element type of the wrapped list
 */
public class ConvertedList<E, W> extends AbstractList<E> {

    private final List<W> list;
    private final Converter<E, W> converter;

    /**
     * Constructor.
     *
     * @param list wrapped list
     * @param converter element converter
     * @throws IllegalArgumentException if either parameter is null
     */
    public ConvertedList(List<W> list, Converter<E, W> converter) {
        Preconditions.checkArgument(list != null, "null list");
        Preconditions.checkArgument(converter != null, "null converter");
        this.list = list;
        this.converter = converter;
    }

    /**
     * Get the wrapped {@link List}.
     *
     * @return the wrapped {@link List}.
     */
    public List<W> getWrappedList() {
        return this.list;
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
    public E get(int index) {
        final W value = this.list.get(index);
        return value != null ? this.converter.reverse().convert(value) : null;
    }

    @Override
    public int size() {
        return this.list.size();
    }

    @Override
    public boolean isEmpty() {
        return this.list.isEmpty();
    }

    @Override
    public E set(int index, E elem) {
        final W welem = elem != null ? this.converter.convert(elem) : null;
        final W prev = this.list.set(index, welem);
        return prev != null ? this.converter.reverse().convert(prev) : null;
    }

    @Override
    public void add(int index, E elem) {
        final W welem = elem != null ? this.converter.convert(elem) : null;
        this.list.add(index, welem);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> elems) {
        return this.list.addAll(index, elems.stream()
          .map(elem -> elem != null ? this.converter.convert(elem) : null)
          .collect(Collectors.toList()));
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
        return this.list.contains(wobj);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int indexOf(Object obj) {
        W wobj = null;
        if (obj != null) {
            try {
                wobj = this.converter.convert((E)obj);
            } catch (ClassCastException e) {
                return -1;
            }
        }
        return this.list.indexOf(wobj);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int lastIndexOf(Object obj) {
        W wobj = null;
        if (obj != null) {
            try {
                wobj = this.converter.convert((E)obj);
            } catch (ClassCastException e) {
                return -1;
            }
        }
        return this.list.lastIndexOf(wobj);
    }

    @Override
    public void clear() {
        this.list.clear();
    }

    @Override
    public E remove(int index) {
        final W welem = this.list.remove(index);
        return welem != null ? this.converter.reverse().convert(welem) : null;
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.transform(this.list.iterator(), this.converter.reverse());
    }

    @Override
    public List<E> subList(int min, int max) {
        return new ConvertedList<>(this.list.subList(min, max), this.converter);
    }

    @Override
    protected void removeRange(int min, int max) {
        this.list.subList(min, max).clear();
    }
}

