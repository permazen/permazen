
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link QueryList} implementation that only actually holds a portion, or "window" of a larger list.
 * If a list element outside the "window" is accessed, an {@link InvalidQueryListException} is thrown,
 * prompting another query.
 *
 * @see AbstractQueryContainer
 */
public class WindowQueryList<T> implements QueryList<T> {

    private final ArrayList<T> window;
    private final int listSize;
    private final int minIndex;
    private final int maxIndex;

    /**
     * Constructor when the original list is given with window bounds.
     *
     * @param list original list from which to cache elements
     * @param minIndex index of the first list element to cache (inclusive)
     * @param maxIndex index of the last list element to cache (exclusive)
     * @throws IllegalArgumentException if {@code list} is null
     * @throws IllegalArgumentException if <code>minIndex &gt; maxIndex</code>
     */
    public WindowQueryList(List<? extends T> list, int minIndex, int maxIndex) {
        if (list == null)
            throw new IllegalArgumentException("null list");
        if (minIndex > maxIndex)
            throw new IllegalArgumentException("minIndex > maxIndex");
        this.listSize = list.size();
        this.minIndex = Math.min(Math.max(minIndex, 0), listSize);
        this.maxIndex = Math.min(Math.max(maxIndex, 0), listSize);
        int windowSize = maxIndex - minIndex;
        this.window = new ArrayList<T>(windowSize);
        for (int i = 0; i < windowSize; i++)
            this.window.set(i, list.get(this.minIndex + i));
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
    public WindowQueryList(int offset, List<? extends T> window, int totalSize) {
        if (window == null)
            throw new IllegalArgumentException("null window");
        if (offset < 0)
            throw new IllegalArgumentException("offset < 0");
        if (offset + window.size() > totalSize)
            throw new IllegalArgumentException("offset + window.size() > totalSize");
        this.window = new ArrayList<T>(window);
        this.listSize = totalSize;
        this.minIndex = offset;
        this.maxIndex = offset + window.size();
    }

    @Override
    public int size() {
        return this.listSize;
    }

    @Override
    public T get(int index) throws InvalidQueryListException {
        if (index < 0 || index >= this.listSize)
            throw new IndexOutOfBoundsException("index = " + index);
        if (index < this.minIndex || index >= this.maxIndex)
            throw new InvalidQueryListException();
        return this.window.get(index - this.minIndex);
    }
}

