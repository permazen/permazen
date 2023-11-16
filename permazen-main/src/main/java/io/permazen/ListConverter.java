
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.util.ConvertedList;

import java.util.List;

/**
 * Converts {@link List}s into {@link ConvertedList}s using the provided element {@link Converter}.
 *
 * @param <E> element type of the converted sets
 * @param <W> element type of the unconverted (wrapped) sets
 */
class ListConverter<E, W> extends Converter<List<E>, List<W>> {

    private final Converter<E, W> elementConverter;

    ListConverter(Converter<E, W> elementConverter) {
        Preconditions.checkArgument(elementConverter != null, "null elementConverter");
        this.elementConverter = elementConverter;
    }

    @Override
    protected List<W> doForward(List<E> set) {
        if (set == null)
            return null;
        return new ConvertedList<W, E>(set, this.elementConverter.reverse());
    }

    @Override
    protected List<E> doBackward(List<W> set) {
        if (set == null)
            return null;
        return new ConvertedList<>(set, this.elementConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ListConverter<?, ?> that = (ListConverter<?, ?>)obj;
        return this.elementConverter.equals(that.elementConverter);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.elementConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[elementConverter=" + this.elementConverter + "]";
    }
}
