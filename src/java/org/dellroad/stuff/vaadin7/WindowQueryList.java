
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.Arrays;
import java.util.List;

/**
 * {@link QueryList} implementation that only actually holds a portion, or "window" of a larger list.
 * If a list element outside the "window" is accessed, an {@link InvalidQueryListException} is thrown,
 * prompting another query.
 *
 * @see AbstractQueryContainer
 */
public class WindowQueryList<T> implements QueryList<T> {

    private final List<? extends T> window;
    private final long offset;
    private final long totalSize;

    /**
     * Constructor when the original list is given with window bounds.
     *
     * @param list original list from which to cache elements
     * @param offset index in the list of the first element in the window
     * @param count number of elements in the window
     * @throws IllegalArgumentException if {@code list} is null
     * @throws IllegalArgumentException if {@code offset} and/or {@code count} are invalid
     */
    @SuppressWarnings("unchecked")
    public WindowQueryList(List<? extends T> list, int offset, int count) {
        if (list == null)
            throw new IllegalArgumentException("null list");
        this.totalSize = list.size();
        if (offset < 0 || offset > this.totalSize)
            throw new IllegalArgumentException("bad offset");
        this.offset = offset;
        if (count < 0 || offset + count > this.totalSize)
            throw new IllegalArgumentException("bad count");

        // Copy elements to allow original list to be freed
        this.window = (List<T>)Arrays.asList(list.subList(offset, offset + count).toArray());
    }

    /**
     * Constructor when a "window" list is given with its position in the original list.
     *
     * @param window list of only those elements in the "window"
     * @param offset offset of the "window" in the original list
     * @param totalSize total size of the original list
     * @throws IllegalArgumentException if {@code window} is null
     * @throws IllegalArgumentException if <code>offset &lt; 0</code>
     * @throws IllegalArgumentException if <code>offset + window.size() &gt; totalSize</code>
     */
    public WindowQueryList(long offset, List<? extends T> window, long totalSize) {
        if (window == null)
            throw new IllegalArgumentException("null window");
        if (offset < 0)
            throw new IllegalArgumentException("offset < 0");
        if (offset + window.size() > totalSize)
            throw new IllegalArgumentException("offset + window.size() > totalSize");
        this.window = window;
        this.offset = offset;
        this.totalSize = totalSize;
    }

    @Override
    public long size() {
        return this.totalSize;
    }

    @Override
    public T get(long index) throws InvalidQueryListException {
        if (index < 0 || index >= this.totalSize)
            throw new IndexOutOfBoundsException("index = " + index);
        if (index < this.offset || index >= this.offset + this.window.size())
            throw new InvalidQueryListException();
        return this.window.get((int)(index - this.offset));
    }
}

