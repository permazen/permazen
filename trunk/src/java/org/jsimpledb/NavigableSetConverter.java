
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import java.util.NavigableSet;

import org.jsimpledb.util.ConvertedNavigableSet;

/**
 * Converts {@link NavigableSet}s into {@link ConvertedNavigableSet}s using the provided element {@link Converter}.
 *
 * @param <E> element type of the converted sets
 * @param <W> element type of the unconverted (wrapped) sets
 */
class NavigableSetConverter<E, W> extends Converter<NavigableSet<E>, NavigableSet<W>> {

    private final Converter<E, W> elementConverter;

    NavigableSetConverter(Converter<E, W> elementConverter) {
        this.elementConverter = elementConverter;
    }

    @Override
    protected NavigableSet<W> doForward(NavigableSet<E> set) {
        if (set == null)
            return null;
        return new ConvertedNavigableSet<W, E>(set, this.elementConverter.reverse());
    }

    @Override
    protected NavigableSet<E> doBackward(NavigableSet<W> set) {
        if (set == null)
            return null;
        return new ConvertedNavigableSet<E, W>(set, this.elementConverter);
    }
}

