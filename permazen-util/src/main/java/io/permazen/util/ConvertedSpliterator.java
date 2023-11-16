
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Provides a transformed view of a wrapped {@link ConvertedSpliterator} using a strictly invertable {@link Converter}.
 *
 * @param <E> element type of this instance
 * @param <W> element type of the wrapped instance
 */
public class ConvertedSpliterator<E, W> implements Spliterator<E> {

    private final Spliterator<W> inner;
    private final Converter<W, E> converter;

    /**
     * Constructor.
     *
     * @param inner wrapped instance
     * @param converter element converter
     * @throws IllegalArgumentException if either parameter is null
     */
    public ConvertedSpliterator(Spliterator<W> inner, Converter<W, E> converter) {
        Preconditions.checkArgument(inner != null, "null inner");
        Preconditions.checkArgument(converter != null, "null converter");
        this.inner = inner;
        this.converter = converter;
    }

    /**
     * Get the wrapped {@link Spliterator}.
     *
     * @return the wrapped {@link Spliterator}.
     */
    public Spliterator<W> getWrappedSpliterator() {
        return this.inner;
    }

    /**
     * Get the {@link Converter} associated with this instance.
     *
     * @return associated {@link Converter}
     */
    public Converter<W, E> getConverter() {
        return this.converter;
    }

// Spliterator

    @Override
    public int characteristics() {
        return this.inner.characteristics();
    }

    @Override
    public long estimateSize() {
        return this.inner.estimateSize();
    }

    @Override
    public void forEachRemaining(Consumer<? super E> consumer) {
        this.inner.forEachRemaining(element -> consumer.accept(this.converter.convert(element)));
    }

    @Override
    public Comparator<E> getComparator() {
        final Comparator<? super W> comparator = this.inner.getComparator();
        return comparator != null ? new ConvertedComparator<E, W>(comparator, this.converter.reverse()) : null;
    }

    @Override
    public long getExactSizeIfKnown() {
        return this.inner.getExactSizeIfKnown();
    }

    @Override
    public boolean hasCharacteristics(int characteristics) {
        return this.inner.hasCharacteristics(characteristics);
    }

    @Override
    public boolean tryAdvance(Consumer<? super E> action) {
        return this.inner.tryAdvance(element -> action.accept(this.converter.convert(element)));
    }

    @Override
    public Spliterator<E> trySplit() {
        final Spliterator<W> split = this.inner.trySplit();
        return split != null ? new ConvertedSpliterator<>(split, this.converter) : null;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ConvertedSpliterator<?, ?> that = (ConvertedSpliterator<?, ?>)obj;
        return this.inner.equals(that.inner) && this.converter.equals(that.converter);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ this.inner.hashCode()
          ^ this.converter.hashCode();
    }
}
