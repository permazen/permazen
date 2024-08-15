
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class ListFieldAddNotifier<E> extends ListFieldNotifier<E> {

    final int index;
    final E newValue;

    ListFieldAddNotifier(ListField<E> field, ObjId id, int index, E newValue) {
        super(field, id);
        this.index = index;
        this.newValue = newValue;
    }

    @Override
    public void notify(Transaction tx, ListFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onListFieldAdd(tx, this.id, this.field, path, referrers, this.index, this.newValue);
    }
}
