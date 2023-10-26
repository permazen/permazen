
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Objects;

/**
 * Comparator that compares using converted values.
 */
class ConvertedComparator<E, W> implements Comparator<E> {

    private final Comparator<? super W> comparator;
    private final Converter<E, W> converter;

    ConvertedComparator(Comparator<? super W> comparator, Converter<E, W> converter) {
        Preconditions.checkArgument(converter != null, "null converter");
        this.comparator = comparator;
        this.converter = converter;
    }

    /**
     * Get the wrapped {@link Comparator}.
     *
     * @return the wrapped {@link Comparator}.
     */
    public Comparator<? super W> getWrappedComparator() {
        return this.comparator;
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
    public int compare(E obj1, E obj2) {
        final W wobj1 = this.converter.convert(obj1);
        final W wobj2 = this.converter.convert(obj2);
        return this.comparator != null ? this.comparator.compare(wobj1, wobj2) : ((Comparable<W>)wobj1).compareTo(wobj2);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ConvertedComparator<?, ?> that = (ConvertedComparator<?, ?>)obj;
        return Objects.equals(this.comparator, that.comparator)
          && this.converter.equals(that.converter);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.comparator) ^ this.converter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[converter=" + this.converter + ",comparator=" + this.comparator + "]";
    }
}
