
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class ListFieldReplaceNotifier<E> extends ListFieldNotifier<E> {

    final int index;
    final E oldValue;
    final E newValue;

    ListFieldReplaceNotifier(ListField<E> field, ObjId id, int index, E oldValue, E newValue) {
        super(field, id);
        this.index = index;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public void notify(Transaction tx, ListFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onListFieldReplace(tx, this.id, this.field, path, referrers, this.index, this.oldValue, this.newValue);
    }
}
