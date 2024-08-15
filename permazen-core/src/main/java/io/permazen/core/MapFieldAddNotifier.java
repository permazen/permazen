
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class MapFieldAddNotifier<K, V> extends MapFieldNotifier<K, V> {

    final K newKey;
    final V newValue;

    MapFieldAddNotifier(MapField<K, V> field, ObjId id, K newKey, V newValue) {
        super(field, id);
        this.newKey = newKey;
        this.newValue = newValue;
    }

    @Override
    public void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onMapFieldAdd(tx, this.id, this.field, path, referrers, this.newKey, this.newValue);
    }
}
