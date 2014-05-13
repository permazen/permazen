
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public abstract class AbstractChannel<T> implements Channel<T> {

    protected final ItemType<T> itemType;

    protected AbstractChannel(Class<T> type) {
        this(new SimpleItemType<T>(type));
    }

    protected AbstractChannel(ItemType<T> itemType) {
        if (itemType == null)
            throw new IllegalArgumentException("null itemType");
        this.itemType = itemType;
    }

    @Override
    public ItemType<T> getItemType() {
        return this.itemType;
    }
}

