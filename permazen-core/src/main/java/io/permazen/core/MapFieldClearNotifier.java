
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class MapFieldClearNotifier<K, V> extends MapFieldNotifier<K, V> {

    MapFieldClearNotifier(MapField<K, V> field, ObjId id) {
        super(field, id);
    }

    @Override
    public void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onMapFieldClear(tx, this.id, this.field, path, referrers);
    }
}
