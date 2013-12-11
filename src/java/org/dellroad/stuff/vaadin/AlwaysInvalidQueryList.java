
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

/**
 * A {@link QueryList} that always throws {@link InvalidQueryListException}.
 */
public class AlwaysInvalidQueryList<T> implements QueryList<T> {

    private final long size;

    /**
     * Constructor.
     *
     * @param size size of this list to report via {@link #size}
     * @throws IllegalArgumentException if {@code size} is negative
     */
    public AlwaysInvalidQueryList(long size) {
        if (size < 0)
            throw new IllegalArgumentException("size < 0");
        this.size = size;
    }

    public long size() {
        return this.size;
    }

    public T get(long index) throws InvalidQueryListException {
        throw new InvalidQueryListException();
    }
}

