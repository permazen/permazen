
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class SetFieldClearNotifier<E> extends SetFieldNotifier<E> {

    SetFieldClearNotifier(SetField<E> field, ObjId id) {
        super(field, id);
    }

    @Override
    public void notify(Transaction tx, SetFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onSetFieldClear(tx, this.id, this.field, path, referrers);
    }
}
