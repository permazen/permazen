
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An {@link Iterator} that is {@link Closeable} and can be {@link Closeable#close close()}'d without
 * throwing any exceptions.
 *
 * @param <E> iteration element type
 */
public interface CloseableIterator<E> extends Iterator<E>, Closeable {

    @Override
    void close();

    /**
     * Wrap the given plain {@link Iterator} as necessary to make it a {@link CloseableIterator}.
     *
     * <p>
     * If {@code iterator} already implements {@link CloseableIterator}, then {@code iterator} is returned.
     *
     * <p>
     * If {@code iterator} does not implement {@link CloseableIterator} but does implement {@link Closeable}
     * (or {@link AutoCloseable}), then the returned instance will delegate to {@code iterator.close()} when
     * {@link Closeable#close close()} is invoked, and any exception thrown will be discarded.
     *
     * <p>
     * Otherwise, invoking {@link Closeable#close close()} on the returned instance does nothing.
     *
     * @param iterator nested iterator
     * @param <E> iteration element type
     * @return {@link CloseableIterator} wrapping {@code iterator}, or null if {@code iterator} is null
     */
    static <E> CloseableIterator<E> wrap(final Iterator<E> iterator) {
        if (iterator == null)
            return null;
        if (iterator instanceof CloseableIterator)
            return (CloseableIterator<E>)iterator;
        return new CloseableIteratorWrapper<>(iterator, iterator instanceof AutoCloseable ? (AutoCloseable)iterator : null);
    }

    /**
     * Wrap the given plain {@link Iterator} to make it a {@link CloseableIterator}, such that when
     * {@link Closeable#close close()} is invoked, the associated {@code resource} is closed.
     *
     * @param iterator nested iterator
     * @param resource resource to close when the returned instance is {@link Closeable#close close()}'d,
     *  or null to do nothing on {@link Closeable#close close()}
     * @param <E> iteration element type
     * @return {@link CloseableIterator} wrapping {@code iterator}, or null if {@code iterator} is null
     */
    static <E> CloseableIterator<E> wrap(final Iterator<E> iterator, AutoCloseable resource) {
        if (iterator == null)
            return null;
        return new CloseableIteratorWrapper<>(iterator, resource);
    }

    /**
     * Return an empty {@link CloseableIterator}.
     *
     * @return empty {@link CloseableIterator}
     */
    static <E> CloseableIterator<E> emptyIterator() {
        return new CloseableIterator<E>() {
            @Override
            public boolean hasNext() {
                return false;
            }
            @Override
            public E next() {
                throw new NoSuchElementException();
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void close() {
            }
        };
    }

    /**
     * Create a {@link Stream} from this iterator, and arrange for this iterator to be closed
     * when the stream is closed.
     *
     * @return this instance wrapped as a {@link Stream}
     */
    default Stream<E> toStream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false).onClose(this::close);
    }
}
