
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class SetFieldAddNotifier<E> extends SetFieldNotifier<E> {

    final E newValue;

    SetFieldAddNotifier(SetField<E> field, ObjId id, E newValue) {
        super(field, id);
        this.newValue = newValue;
    }

    @Override
    public void notify(Transaction tx, SetFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onSetFieldAdd(tx, this.id, this.field, path, referrers, this.newValue);
    }
}
