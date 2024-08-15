
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class DeleteNotifier extends Notifier<DeleteListener> {

    DeleteNotifier(ObjId id) {
        super(id);
    }

    @Override
    public Class<DeleteListener> getListenerType() {
        return DeleteListener.class;
    }

    @Override
    public void notify(Transaction tx, DeleteListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onDelete(tx, this.id, path, referrers);
    }
}
