
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.Collection;
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

    public static final int DEFAULT_WINDOW_SIZE = 50;

    private final int windowSize;

    private long size;

// Constructors

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote><code>
     *      {@link #AbstractUnsizedContainer(int) AbstractUnsizedContainer}({@link #DEFAULT_WINDOW_SIZE})
     *  </code></blockquote>
     *
     * <p>
     * After using this constructor, subsequent invocations of {@link #setPropertyExtractor setPropertyExtractor()}
     * and {@link #setProperties setProperties()} are required to define the properties of this container
     * and how to extract them.
     * </p>
     */
    protected AbstractUnsizedContainer() {
        this(DEFAULT_WINDOW_SIZE);
    }

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote><code>
     *      {@link #AbstractUnsizedContainer(int, PropertyExtractor) AbstractUnsizedContainer}({@link #DEFAULT_WINDOW_SIZE},
     *          propertyExtractor)
     *  </code></blockquote>
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setProperties setProperties()} is required
     * to define the properties of this container.
     * </p>
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     */
    protected AbstractUnsizedContainer(PropertyExtractor<? super T> propertyExtractor) {
        this(DEFAULT_WINDOW_SIZE, propertyExtractor);
    }

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote><code>
     *      {@link #AbstractUnsizedContainer(int, PropertyExtractor, Collection) AbstractUnsizedContainer}({@link
     *          #DEFAULT_WINDOW_SIZE}, propertyExtractor, propertyDefs)
     *  </code></blockquote>
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     * @param propertyDefs container property definitions; null is treated like the empty set
     */
    protected AbstractUnsizedContainer(PropertyExtractor<? super T> propertyExtractor,
      Collection<? extends PropertyDef<?>> propertyDefs) {
        this(DEFAULT_WINDOW_SIZE, propertyExtractor, propertyDefs);
    }

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, subsequent invocations of {@link #setPropertyExtractor setPropertyExtractor()}
     * and {@link #setProperties setProperties()} are required to define the properties of this container
     * and how to extract them.
     * </p>
     *
     * @param windowSize size of the query window
     * @throws IllegalArgumentException if {@code windowSize} is less than 1
     */
    protected AbstractUnsizedContainer(int windowSize) {
        this(windowSize, null);
    }

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setProperties setProperties()} is required
     * to define the properties of this container.
     * </p>
     *
     * @param windowSize size of the query window
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     * @throws IllegalArgumentException if {@code windowSize} is less than 1
     */
    protected AbstractUnsizedContainer(int windowSize, PropertyExtractor<? super T> propertyExtractor) {
        this(windowSize, propertyExtractor, null);
    }

    /**
     * Primary constructor.
     *
     * @param windowSize size of the query window (how big of a chunk we query at one time)
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     * @param propertyDefs container property definitions; null is treated like the empty set
     * @throws IllegalArgumentException if {@code windowSize} is less than 1
     */
    protected AbstractUnsizedContainer(int windowSize, PropertyExtractor<? super T> propertyExtractor,
      Collection<? extends PropertyDef<?>> propertyDefs) {
        if (windowSize < 1)
            throw new IllegalArgumentException("windowSize = " + windowSize);
        this.windowSize = windowSize;
        this.setPropertyExtractor(propertyExtractor);
        this.setProperties(propertyDefs);
    }

    @Override
    protected QueryList<T> query(long hint) {

        // Clip hint value
        hint = Math.min(hint, this.size - 1);
        hint = Math.max(hint, 0);

        // Query underlying data for the window of items centered at "hint"
        final long offset = Math.max(0, hint - this.windowSize / 2);
        final List<? extends T> window = this.queryWindow(offset, this.windowSize);
        final int querySize = window.size();

        // Update size estimate
        if (querySize <= 0)                     // the underlying data has shrunk on us
            this.size = this.getSmallerEstimate(offset);
        else if (querySize < this.windowSize)   // we overlapped the end; now we know the exact size
            this.size = offset + querySize;
        else {                                   // we are somewhere in the middle

            // Get estimate of new larger size
            final long lowerBound = offset + querySize;
            long largerSize = this.getLargerEstimate(lowerBound);

            // Ensure that it is at least large enough to avoid causing another, redundant subsequent resize. This also
            // ensures that it is strictly larger than the last item in our window so we can always trigger another query.
            largerSize = Math.max(largerSize, lowerBound + this.windowSize);

            // Increase size estimate (maybe)
            this.size = Math.max(this.size, largerSize);
        }

        // Return QueryList
        return new WindowQueryList<T>(offset, window, this.size);
    }

    /**
     * Get the window size configured at construction time.
     */
    public int getWindowSize() {
        return this.windowSize;
    }

    /**
     * Get the current size estimate for the underlying data.
     */
    public long getCurrentSizeEstimate() {
        return this.size;
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
     * The implementation in {@link AbstractUnsizedContainer} returns {@code upperBound * 0.75}.
     * </p>
     *
     * @param upperBound an upper bound on the size of the underlying data
     * @return estimate of the actual size of the underlying data
     */
    protected long getSmallerEstimate(long upperBound) {
        return (upperBound >> 1) + (upperBound >> 2);
    }

    /**
     * Estimate the size of the underlying data given that {@code lowerBound} is a lower bound.
     * This effectively determines how much data will appear to be "beyond" the current window.
     *
     * <p>
     * The implementation in {@link AbstractUnsizedContainer} returns {@code lowerBound * 1.25}.
     * </p>
     *
     * @param lowerBound a lower bound on the size of the underlying data
     * @return estimate of the actual size of the underlying data
     */
    protected long getLargerEstimate(long lowerBound) {
        return lowerBound + (lowerBound >> 2);
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

