
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class SetFieldRemoveNotifier<E> extends SetFieldNotifier<E> {

    final E oldValue;

    SetFieldRemoveNotifier(SetField<E> field, ObjId id, E oldValue) {
        super(field, id);
        this.oldValue = oldValue;
    }

    @Override
    public void notify(Transaction tx, SetFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onSetFieldRemove(tx, this.id, this.field, path, referrers, this.oldValue);
    }
}
