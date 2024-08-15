
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class ListFieldClearNotifier<E> extends ListFieldNotifier<E> {

    ListFieldClearNotifier(ListField<E> field, ObjId id) {
        super(field, id);
    }

    @Override
    public void notify(Transaction tx, ListFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onListFieldClear(tx, this.id, this.field, path, referrers);
    }
}
