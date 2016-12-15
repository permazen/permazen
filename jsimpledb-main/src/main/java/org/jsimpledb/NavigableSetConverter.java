
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

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
        Preconditions.checkArgument(elementConverter != null, "null elementConverter");
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
        return new ConvertedNavigableSet<>(set, this.elementConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final NavigableSetConverter<?, ?> that = (NavigableSetConverter<?, ?>)obj;
        return this.elementConverter.equals(that.elementConverter);
    }

    @Override
    public int hashCode() {
        return this.elementConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[elementConverter=" + this.elementConverter + "]";
    }
}

