
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Support superclass for {@link Set} implementations based on database entries.
 *
 * <p>
 * This class assumes the following:
 * <ul>
 *  <li>The size of the set is not cached, i.e., it requires an actual enumeration of all of the set's elements; and
 *  <li>Iteration may utilize a resource that needs to be closed
 * </ul>
 *
 * <p>
 * As a result:
 * <ul>
 *  <li>{@link AbstractSet} methods that rely on {@link #size} are overridden with implementations
 *      that avoid the use of {@link #size} where possible; and
 *  <li>{@link #iterator} returns a {@link CloseableIterator}
 *  <li>{@link #stream}, which is based on {@link #iterator}, arranges (via {@link Stream#onClose Stream.onClose()})
 *      for the iterator to be closed on {@link Stream#close Stream.close()}.
 * </ul>
 * To be safe, try-with-resources should be used with {@link #iterator} and {@link #stream}.
 *
 * <p>
 * For a read-only implementation, subclasses should implement {@link #contains contains()} and {@link #iterator iterator()}.
 *
 * <p>
 * For a mutable implementation, subclasses should also implement {@link #add add()}, {@link #remove remove()},
 * and {@link #clear clear()}, and make the {@link #iterator iterator()} mutable.
 */
public abstract class AbstractIterationSet<E> extends AbstractSet<E> {

    protected AbstractIterationSet() {
    }

    @Override
    public abstract CloseableIterator<E> iterator();

    /**
     * Calculate size.
     *
     * <p>
     * The implementation in {@link AbstractIterationSet} iterates through all of the elements.
     */
    @Override
    public int size() {
        try (CloseableIterator<E> i = this.iterator()) {
            int count = 0;
            while (i.hasNext()) {
                i.next();
                count++;
            }
            return count;
        }
    }

    /**
     * Overridden in {@link AbstractIterationSet} to minimize the use of {@link #size}.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!(obj instanceof Set))
            return false;
        final Set<?> that = (Set<?>)obj;
        final Iterator<?> i2 = that.iterator();
        try (CloseableIterator<?> i1 = this.iterator()) {
            while (true) {
                final boolean hasNext1 = i1.hasNext();
                final boolean hasNext2 = i2.hasNext();
                if (!hasNext1 && !hasNext2)
                    return true;
                if (!hasNext1 || !hasNext2)
                    return false;
                if (!this.contains(i2.next()))
                    return false;
                i1.next();
            }
        } finally {
            if (i2 instanceof CloseableIterator)
                ((CloseableIterator<?>)i2).close();
        }
    }

    // This is here to silence a checkstyle warning
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Overridden in {@link AbstractIterationSet} to minimize the use of {@link #size}.
     */
    @Override
    public boolean isEmpty() {
        return !this.iterator().hasNext();
    }

    /**
     * Overridden in {@link AbstractIterationSet} to minimize the use of {@link #size}.
     */
    @Override
    public Object[] toArray() {
        return this.toArray(new Object[0]);
    }

    /**
     * Overridden in {@link AbstractIterationSet} to minimize the use of {@link #size}.
     */
    @Override
    public <T> T[] toArray(T[] array) {
        final ArrayList<E> list = new ArrayList<>();
        try (CloseableIterator<E> i = this.iterator()) {
            while (i.hasNext())
                list.add(i.next());
        }
        return list.toArray(array);
    }

    /**
     * Overridden in {@link AbstractIterationSet} to avoid the use of {@link #size}.
     *
     * <p>
     * Note: the underlying {@link CloseableIterator} is <i>not</i> closed when this method is used.
     * Prefer using {@link #buildSpliterator} and arranging for the iterator to be closed separately.
     */
    @Override
    public Spliterator<E> spliterator() {
        return this.buildSpliterator(this.iterator());
    }

    /**
     * Build a {@link Spliterator} appropriate for this set from the given instance iterator.
     *
     * <p>
     * Implementations should probably use {@link Spliterators#spliteratorUnknownSize(Iterator, int)
     * Spliterators.spliteratorUnknownSize()} unless the size is known.
     *
     * @param iterator a new iterator returned from {#link #iterator}
     */
    protected Spliterator<E> buildSpliterator(Iterator<E> iterator) {
        return Spliterators.spliteratorUnknownSize(iterator, Spliterator.DISTINCT);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The implementation in {@link AbstractIterationSet} build a stream from the results from {@link #iterator}
     * and {@link #buildSpliterator buildSpliterator()}, and marks the iterator for close via
     * {@link Stream#onClose Stream.onClose()}.
     */
    @Override
    public Stream<E> stream() {
        final CloseableIterator<E> iterator = this.iterator();
        final Spliterator<E> spliterator = this.buildSpliterator(iterator);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }
}
