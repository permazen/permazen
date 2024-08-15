
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class MapFieldRemoveNotifier<K, V> extends MapFieldNotifier<K, V> {

    final K oldKey;
    final V oldValue;

    MapFieldRemoveNotifier(MapField<K, V> field, ObjId id, K oldKey, V oldValue) {
        super(field, id);
        this.oldKey = oldKey;
        this.oldValue = oldValue;
    }

    @Override
    public void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onMapFieldRemove(tx, this.id, this.field, path, referrers, this.oldKey, this.oldValue);
    }
}
