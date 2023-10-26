
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.function.Predicate;

/**
 * Allows efficient forward and backward paging through a {@link NavigableSet}.
 *
 * <p>
 * Instances are configured with a page size and a view ordering (ascending or descending).
 *
 * <p>
 * Instances maintain a cursor position that anchors one "page" of consecutive items within the set. Each time
 * {@link #readCurrentPage} is invoked, the set is reacquired from {@link #getNavigableSet} and the
 * contents of the current page are read from it.
 *
 * <p>
 * For navigation, {@link #nextPage} and {@link #prevPage} move to the next or previous page,
 * {@link #firstPage} and {@link #lastPage} jump to the first or last page.
 * After movement, {@link #hasNextPage} and {@link #hasPrevPage} indicate whether adjacent pages exist.
 *
 * <p>
 * The {@linkplain #setDescending view ordering} can be either ascending (default) or descending. With descending view ordering,
 * the set is viewed in reverse, i.e., as if viewing the result of {@link NavigableSet#descendingSet}.
 *
 * @param <E> set element type
 */
public abstract class NavigableSetPager<E> {

    /**
     * Default page size ({@value #DEFAULT_PAGE_SIZE}).
     *
     * @see #setPageSize setPageSize()
     */
    public static final int DEFAULT_PAGE_SIZE = 100;

    private int pageSize = DEFAULT_PAGE_SIZE;
    private boolean descending;                                 // query ordering
    private Predicate<? super E> filter;                        // query filter
    private Bound anchor;                                       // current page min (forward) or max (backward)
    private Bound limit;                                        // marks the end of the current page, if known
    private boolean backwards;                                  // most recent move was backward (i.e., prev() instead of next())
    private boolean dataBefore;                                 // there is more data behind current page
    private boolean dataAfter;                                  // there is more data ahead of current page
    private int pageNumber = 1;

// Configuration

    /**
     * Get the page size.
     *
     * @return maximum number of rows in a page, always greater than zero
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * Set the page size.
     *
     * @param pageSize maximum number of rows in a page
     * @throws IllegalArgumentException if {@code pageSize} is zero or less
     */
    public void setPageSize(int pageSize) {
        Preconditions.checkArgument(pageSize > 0, "pageSize <= 0");
        this.pageSize = pageSize;
    }

    /**
     * Get the view ordering. Default is ascending.
     *
     * @return false if container order is ascending, true if descending
     */
    public boolean isDescending() {
        return this.descending;
    }

    /**
     * Set the view ordering.
     *
     * @param descending true for descending view, false for ascending view
     */
    public void setDescending(boolean descending) {
        if (this.descending != descending) {
            this.descending = descending;
            this.limit = null;
            final int backwardOffset = this.backwards ? 0 : 1;
            if (this.pageNumber > backwardOffset)
                this.pageNumber = -this.pageNumber + backwardOffset;
            else if (this.pageNumber < -backwardOffset)
                this.pageNumber = -this.pageNumber + backwardOffset;
        }
    }

    /**
     * Reverse the view ordering.
     *
     * <p>
     * Equivalent to {@code this.setDescending(!this.isDescending())}.
     */
    public void reverseViewOrdering() {
        this.setDescending(!this.descending);
    }

    /**
     * Filter which items in the set are returned.
     *
     * <p>
     * Items that fail to pass the specified {@code filter} are omitted from the results
     * and do not contribute to the page total. Beware that a filter can reject arbitrarily
     * many items and therefore when using filters the time it takes to load a full page
     * is potentially unbounded.
     *
     * @param filter filter that accepts only the desired items, or null to accept all
     */
    public void setFilter(Predicate<? super E> filter) {
        this.filter = filter;
    }

// Paging

    /**
     * Indicates that there are more results after the current page.
     *
     * @return true if there are more results in the forward direction, false if we are on the last page
     */
    public boolean hasNextPage() {
        return this.backwards ? this.dataBefore : this.dataAfter;
    }

    /**
     * Indicates that there are more results before the current page.
     *
     * @return true if there are more results in the reverse direction, false if we are on the first page
     */
    public boolean hasPrevPage() {
        return this.backwards ? this.dataAfter : this.dataBefore;
    }

    /**
     * Advance forward to the next page of results, if any.
     *
     * <p>
     * This will advance to the next higher page of results if configured for an ascending view,
     * or the next lower page of results if configured for a descending view.
     *
     * <p>
     * After invoking this method, {@link #readCurrentPage} must be invoked for it to take effect.
     * Duplicate invocations of this method without an intervening call to {@link #readCurrentPage}
     * will have no effect and return false.
     *
     * @return true if successful, false if we were already on the last page
     */
    public boolean nextPage() {
        return this.step(false);
    }

    /**
     * Advance backward to the previous page of results, if any.
     *
     * <p>
     * This will regress to be the next lower page of results if configured for an ascending view,
     * or the next higher page of results if configured for a descending view.
     *
     * <p>
     * After invoking this method, {@link #readCurrentPage} must be invoked for it to take effect.
     * Duplicate invocations of this method without an intervening call to {@link #readCurrentPage}
     * will have no effect and return false.
     *
     * @return true if successful, false if we were already on the first page
     */
    public boolean prevPage() {
        return this.step(true);
    }

    private boolean step(boolean backwards) {
        if (!(backwards ^ this.backwards)) {        // move "forward" to next page, same direction
            if (this.limit == null)
                return false;
            this.anchor = this.limit;
        } else {                                    // move "back" to previous page, changing direction
            if (this.anchor == null)
                return false;
            this.backwards = backwards;
        }
        this.limit = null;
        if (this.pageNumber != 0) {
            if (backwards) {
                if (--this.pageNumber == 0)
                    this.pageNumber++;
            } else {
                if (++this.pageNumber == 0)
                    this.pageNumber--;
            }
        }
        return true;
    }

    /**
     * Jump to the first page.
     */
    public void firstPage() {
        this.jump(false);
    }

    /**
     * Jump to the last page.
     */
    public void lastPage() {
        this.jump(true);
    }

    private void jump(boolean last) {
        this.anchor = null;
        this.limit = null;
        this.backwards = last;
        this.dataBefore = last;
        this.dataAfter = !last;
        this.pageNumber = last ? -1 : 1;
    }

    /**
     * Set the current cursor position, i.e., page anchor.
     *
     * <p>
     * The next page returned by {@link #readCurrentPage} will start at {@code cursor},
     * inclusive for an ascending ordering or exclusive for a descending ordering.
     *
     * <p>
     * The {@code cursor} must not be null unless the underlying {@link NavigableSet} supports null values.
     *
     * @param cursor new cursor position for the current page
     */
    public void setCursor(E cursor) {
        this.anchor = new Bound(cursor);
        this.limit = null;
        this.backwards = false;
        this.dataBefore = true;
        this.dataAfter = true;
        this.pageNumber = 0;
    }

    /**
     * Get the current page number, if known.
     *
     * <p>
     * This method returns either a positive or negative number depending on whether the start or the end of the data
     * has been most recently reached. Positive values (1, 2, 3, ...) count pages from the start of the data; negative
     * values (-1, -2, -3, ...) count pages from the end of the data.
     *
     * <p>
     * If {@link #setCursor} has been invoked since the last time we hit the beginning or the end of the data,
     * then the current page number is unknown and this method returns zero.
     *
     * <p>
     * The correctness of this method depends on the underlying data not changing. For example, if {@link #nextPage} is invoked
     * five times and then {@link #prevPage} is invoked two times, this method returns a value three higher than before,
     * regardless of whether items were concurrently added or removed from the underlying set. The page number is only
     * guaranteed to be accurate if the set hasn't changed since we most recently reached the start or end of the data.
     *
     * @return current page number, or zero if unknown
     */
    public int getPageNumber() {
        return this.pageNumber;
    }

    /**
     * Read the contents of the current page.
     *
     * <p>
     * The list is read starting from the current cursor position (inclusive for an ascending ordering,
     * exclusive for a descending ordering).
     *
     * <p>
     * If configured for an ascending view, the list will have ascending ordering;
     * if configured for a descending view, the list will have descending ordering.
     *
     * @return list of items in the current page
     */
    public List<E> readCurrentPage() {

        // Get set
        NavigableSet<E> set = this.getNavigableSet();

        // Set overall ordering
        if (this.descending)
            set = set.descendingSet();

        // Set page anchor point, which determines the first object on the page
        NavigableSet<E> anchorSet;
        if (this.anchor != null) {
            anchorSet = this.backwards ?
              set.headSet(this.anchor.getValue(), false) :                      // anchor point is maximum
              set.tailSet(this.anchor.getValue(), true);                        // anchor point is minimum
        } else
            anchorSet = set;

        // Iterate away from the anchor point in the same direction we're "moving"
        if (this.backwards)
            anchorSet = anchorSet.descendingSet();

        // Gather up to one page's worth of data and remember the limit we reach, if any, to become the next anchor point
        this.limit = null;
        List<E> page = new ArrayList<>(this.pageSize);
        for (E item : anchorSet) {
            if (this.filter != null && !this.filter.test(item))
                continue;
            if (this.backwards)
                page.add(item);
            if (page.size() >= this.pageSize) {
                this.limit = new Bound(item);
                break;
            }
            if (!this.backwards)
                page.add(item);
        }

        // If we immediately ran out of items, bounce off the end and load a full page going in the opposite direction
        if (this.anchor != null && this.limit == null && page.isEmpty()) {
            this.anchor = null;
            this.backwards = !this.backwards;
            this.pageNumber = this.backwards ? -1 : 1;
            return this.readCurrentPage();
        }

        // Determine if there is any data before the anchor point
        if (this.anchor != null) {
            this.dataBefore = this.backwards ?
              !set.tailSet(this.anchor.getValue(), true).isEmpty() :
              !set.headSet(this.anchor.getValue(), false).isEmpty();
        } else
            this.dataBefore = false;

        // Determine if there is any data after the page limit
        if (this.limit != null) {
            this.dataAfter = this.backwards ?
              !set.headSet(this.limit.getValue(), false).isEmpty() :
              true;                             // we already know there's one because we read it and it became this.limit
        } else
            this.dataAfter = false;

        // Un-do the effect of previous reversal
        if (this.backwards)
            page = Lists.reverse(page);

        // Done
        return page;
    }

// Subclass methods

    /**
     * Get the {@link NavigableSet} through which to page.
     *
     * <p>
     * This method is invoked anew each time {@link #readCurrentPage} is invoked.
     *
     * @return the entire query domain in ascending order
     */
    protected abstract NavigableSet<E> getNavigableSet();

// Bound

    private class Bound {

        private final E value;

        Bound(E value) {
            this.value = value;
        }

        public E getValue() {
            return this.value;
        }
    }
}
