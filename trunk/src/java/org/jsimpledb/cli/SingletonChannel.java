
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Comparator;
import java.util.NavigableSet;

import org.jsimpledb.util.NavigableSets;

public abstract class SingletonChannel<T> extends AbstractChannel<T> {

    private final Comparator<? super T> comparator;

    private NavigableSet<T> set;

    public SingletonChannel(Class<T> type) {
        this(new SimpleItemType<T>(type));
    }

    public SingletonChannel(ItemType<T> itemType) {
        this(itemType, null);
    }

    public SingletonChannel(ItemType<T> itemType, Comparator<? super T> comparator) {
        super(itemType);
        this.comparator = comparator;
    }

    @Override
    public final NavigableSet<T> getItems(Session session) {
        if (this.set == null)
            this.set = NavigableSets.singleton(this.comparator, this.getItem(session));
        return this.set;
    }

    protected abstract T getItem(Session session);
}

