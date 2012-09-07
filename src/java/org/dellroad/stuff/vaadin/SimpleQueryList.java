
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import java.util.List;

/**
 * Simple {@link QueryList} implementation using a normal {@link List}.
 *
 * @see AbstractQueryContainer
 */
public class SimpleQueryList<T> implements QueryList<T> {

    private final List<? extends T> list;

    /**
     * Constructor.
     *
     * @param list backing list
     * @throws IllegalArgumentException if {@code list} is null
     */
    public SimpleQueryList(List<? extends T> list) {
        if (list == null)
            throw new IllegalArgumentException("null list");
        this.list = list;
    }

    @Override
    public int size() {
        return this.list.size();
    }

    @Override
    public T get(int index) {
        return this.list.get(index);
    }
}

