
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Set;

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
    ConvertedSet(Set<W> set, Converter<E, W> converter) {
        if (set == null)
            throw new IllegalArgumentException("null set");
        if (converter == null)
            throw new IllegalArgumentException("null converter");
        this.set = set;
        this.converter = converter;
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
    public Iterator<E> iterator() {
        return Iterators.transform(this.set.iterator(), this.converter.reverse());
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
}

