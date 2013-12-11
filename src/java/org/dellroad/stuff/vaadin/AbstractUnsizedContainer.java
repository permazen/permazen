
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.data.Container;

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
 * to schedule a (non-reentrant) property set change notification. Once the end of the underlying data is reached,
 * the size is known.
 * </p>
 *
 * <p>
 * If the actual size of the underlying data is constant, this class will eventually find it. If the
 * actual size of the underlying data can change (either up or down), this class will adapt accordingly,
 * but only when it learns of the new size through an invocation of {@link #queryWindow queryWindow()};
 * as this depends on how the container is used, this may not occur for a long time.
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
    private boolean sizeIsKnown;

// Constructors

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote>
     *      {@link #AbstractUnsizedContainer(int) AbstractUnsizedContainer}{@code (}{@link #DEFAULT_WINDOW_SIZE}{@code )}
     *  </blockquote>
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
     *  <blockquote>
     *      {@link #AbstractUnsizedContainer(int, PropertyExtractor)
     *          AbstractUnsizedContainer}{@code (}{@link #DEFAULT_WINDOW_SIZE}{@code , propertyExtractor)}
     *  </blockquote>
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
     *  <blockquote>
     *      {@link #AbstractUnsizedContainer(int, PropertyExtractor)
     *          AbstractUnsizedContainer}{@code (}{@link #DEFAULT_WINDOW_SIZE}{@code , propertyDefs)}
     *  </blockquote>
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setPropertyExtractor setPropertyExtractor()}
     * is required to define how to extract the properties of this container; alternately, subclasses can override
     * {@link #getPropertyValue getPropertyValue()}.
     * </p>
     *
     * @param propertyDefs container property definitions; null is treated like the empty set
     */
    protected AbstractUnsizedContainer(Collection<? extends PropertyDef<?>> propertyDefs) {
        this(DEFAULT_WINDOW_SIZE, propertyDefs);
    }

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote>
     *      {@link #AbstractUnsizedContainer(int, PropertyExtractor, Collection) AbstractUnsizedContainer}{@code (}{@link
     *          #DEFAULT_WINDOW_SIZE}{@code , propertyExtractor, propertyDefs)}
     *  </blockquote>
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
        this(windowSize, (PropertyExtractor<? super T>)null);
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
     * Constructor.
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setPropertyExtractor setPropertyExtractor()}
     * is required to define how to extract the properties of this container; alternately, subclasses can override
     * {@link #getPropertyValue getPropertyValue()}.
     * </p>
     *
     * @param windowSize size of the query window
     * @param propertyDefs container property definitions; null is treated like the empty set
     * @throws IllegalArgumentException if {@code windowSize} is less than 1
     */
    protected AbstractUnsizedContainer(int windowSize, Collection<? extends PropertyDef<?>> propertyDefs) {
        this(windowSize, null, propertyDefs);
    }

    /**
     * Constructor.
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
        super(propertyExtractor, propertyDefs);
        if (windowSize < 1)
            throw new IllegalArgumentException("windowSize = " + windowSize);
        this.windowSize = windowSize;
    }

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote>
     *      {@link #AbstractUnsizedContainer(int, Class)
     *          AbstractUnsizedContainer}{@code (}{@link #DEFAULT_WINDOW_SIZE}{@code , type)}
     *  </blockquote>
     *
     * @param type class to introspect for {@link ProvidesProperty &#64;ProvidesProperty}-annotated fields and methods
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if {@code type} has two {@link ProvidesProperty &#64;ProvidesProperty}
     *  or {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated methods for the same property
     * @throws IllegalArgumentException if a {@link ProvidesProperty &#64;ProvidesProperty}-annotated method with no
     *  {@linkplain ProvidesProperty#value property name specified} has a name which cannot be interpreted as a bean
     *  property "getter" method
     * @see ProvidesProperty
     * @see ProvidesPropertySort
     * @see ProvidesPropertyScanner
     */
    protected AbstractUnsizedContainer(Class<? super T> type) {
        this(DEFAULT_WINDOW_SIZE, type);
    }

    /**
     * Constructor.
     *
     * <p>
     * Properties will be determined by the {@link ProvidesProperty &#64;ProvidesProperty} and
     * {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated methods in the given class.
     * </p>
     *
     * @param windowSize size of the query window
     * @param type class to introspect for annotated methods
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if {@code type} has two {@link ProvidesProperty &#64;ProvidesProperty}
     *  or {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated methods for the same property
     * @throws IllegalArgumentException if a {@link ProvidesProperty &#64;ProvidesProperty}-annotated method with no
     *  {@linkplain ProvidesProperty#value property name specified} has a name which cannot be interpreted as a bean
     *  property "getter" method
     * @see ProvidesProperty
     * @see ProvidesPropertySort
     * @see ProvidesPropertyScanner
     */
    protected AbstractUnsizedContainer(int windowSize, Class<? super T> type) {
        super(type);
        if (windowSize < 1)
            throw new IllegalArgumentException("windowSize = " + windowSize);
        this.windowSize = windowSize;
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

        // Handle the case where we got an empty window - so the underlying data must have shrunk
        if (querySize <= 0) {

            // If our starting offset was zero, then we know the size must be zero
            if (offset == 0) {
                this.sizeIsKnown = true;
                this.size = 0;
            } else {

                // Guess at what the smaller size is and return a QueryList that will throw an InvalidQueryListException,
                // thereby trigging another probe with a smaller offset. This cycle will repeat until the size is found.
                this.sizeIsKnown = false;
                this.size = this.getSmallerEstimate(offset);
                return new AlwaysInvalidQueryList<T>(this.size);
            }
        }

        // Handle the case where we overlapped the end; now we know the exact size
        if (querySize < this.windowSize) {                  // we overlapped the end; now we know the exact size
            this.size = offset + querySize;
            this.sizeIsKnown = true;
            return new WindowQueryList<T>(offset, window, this.size);
        }

        // Handle the case where we are somewhere in the middle
        if (!this.sizeIsKnown) {

            // Get lower bound on size estimate
            final long lowerBound = offset + querySize;

            // If current size is less than lowerBound + 1, need to expand (the +1 is to leave room to trigger a requery)
            if (this.size < lowerBound + 1) {

                // Get estimate of new larger size
                long largerSize = this.getLargerEstimate(lowerBound);

                // Ensure that it is at least large enough to avoid causing another, redundant subsequent resize. This also
                // ensures that it is strictly larger than the last item in our window so we can always trigger another query.
                largerSize = Math.max(largerSize, lowerBound + this.windowSize);

                // Increase size estimate (maybe)
                this.size = Math.max(this.size, largerSize);
            }
        } else {                                            // we are somewhere in the middle and size is known

            // Detect if the size has grown
            this.size = Math.max(this.size, offset + querySize);
        }

        // Return QueryList based on the window of data we found
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
     * This method should guess at the new value and, when called repeatedly, should converge rapidly to zero.
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

    @Override
    protected void fireItemSetChange(Container.ItemSetChangeEvent event) {
        this.sizeIsKnown = false;
        super.fireItemSetChange(event);
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

