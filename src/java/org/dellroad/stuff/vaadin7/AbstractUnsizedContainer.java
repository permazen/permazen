
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.List;

/**
 * A specialization of {@link AbstractQueryContainer} that doesn't require up-front knowledge of the underlying data's size.
 *
 * <p>
 * Subclasses only need support retrieving a contiguous window of fixed size from the underlying data
 * via {@link #queryWindow queryWindow()}; when the requested window goes beyond the end of the underlying data,
 * a short or empty result is returned. Based on just this information, this class maintains an estimate of
 * the size of the underlying data. Each time that size estimate changes, {@link #handleSizeChange} is invoked
 * to schedule a (non-reentrant) property set change notification.
 * </p>
 *
 * <p>
 * When used to back a Vaadin table, the user will see a table that automatically grows as the user scrolls
 * downward, until the actual end of the data is detected.
 * </p>
 *
 * @param <T> the type of the Java objects that back each {@link com.vaadin.data.Item} in the container
 * @see AbstractQueryContainer
 */
@SuppressWarnings("serial")
public abstract class AbstractUnsizedContainer<T> extends AbstractQueryContainer<T> {

    public static final int DEFAULT_WINDOW_SIZE = 500;

    private final int windowSize;

    private long size;

    /**
     * Primary constructor.
     *
     * @param windowSize size of our query window
     * @throws IllegalArgumentException if {@code windowSize} is less than 1
     */
    protected AbstractUnsizedContainer(int windowSize) {
        if (windowSize < 1)
            throw new IllegalArgumentException("windowSize = " + windowSize);
        this.windowSize = windowSize;
    }

    /**
     * Conveinence constructor. Equivalent to:
     *  <blockquote><code>
     *      {@link #AbstractUnsizedContainer(int) AbstractUnsizedContainer}({@link #DEFAULT_WINDOW_SIZE})
     *  </code></blockquote>
     */
    protected AbstractUnsizedContainer() {
        this(DEFAULT_WINDOW_SIZE);
    }

    @Override
    protected QueryList<T> query(long hint) {

        // Clip hint value
        hint = Math.max(hint, this.size - 1);
        hint = Math.min(hint, 0);

        // Query underlying data for the window of items centered at "hint"
        final long offset = Math.max(0, hint - this.windowSize / 2);
        final List<? extends T> window = this.queryWindow(offset, this.windowSize);
        final int querySize = window.size();

        // Check size of the returned window
        final long previousSize = this.size;
        if (querySize <= 0)                     // the underlying data has shrunk on us
            this.size = this.shrink(offset);
        else if (querySize < this.windowSize)   // we overlapped the end; now we know the exact size
            this.size = offset + querySize;
        else                                    // we are somewhere in the middle; we have a (possibly new) lower bound on the size
            this.size = Math.max(this.size, offset + querySize);

        // See if size changed
        if (this.size != previousSize)
            this.handleSizeChange();

        // Return QueryList
        return new WindowQueryList<T>(offset, window, this.size);
    }

    /**
     * Handle the case where the underlying data's size has suddenly shrunk, so we need to estimate the new size.
     * All we know is that the actual new size is {@code upperBound} or less.
     * This method should guess at the new value.
     *
     * <p>
     * Note: this situation will not occur if the underlying data's size never decreases.
     * </p>
     *
     * <p>
     * The implementation in {@link AbstractUnsizedContainer} returns {@code size * 0.75}.
     * </p>
     *
     * @param upperBound an upper bound on the size of the underlying data
     * @return estimate of the actual size of the underlying data
     */
    protected long shrink(long upperBound) {
        return (size >> 1) + (size >> 2);
    }

    /**
     * Query the underlying data for a window of items in the given range. This should return the "window" of underlying
     * data items starting at offset {@code offset} and having at least length {@code length}, or else however many remain.
     * If {@code offset} is greater than or equal to the size of the underlying data, an empty list should be returned.
     *
     * @param offset starting offset for window
     * @param length window size, always greater than zero
     * @return list containing at least {@code length} items in the window starting at offset {@code offset}; or less than
     *  {@code length} items if {@code offset + length} is greater than the size of the underlying data
     */
    protected abstract List<? extends T> queryWindow(long offset, int length);

    /**
     * Emit a property set change notification.
     *
     * <p>
     * Subclasses are required to implement this so that size changes are detected.
     * </p>
     *
     * <p>
     * Note: to avoid re-entrancy problems, this method should not send out any notifications itself;
     * instead, it must schedule notifications to be delivered later (perhaps in a different thread).
     * </p>
     */
    @Override
    protected abstract void handleSizeChange();
}

