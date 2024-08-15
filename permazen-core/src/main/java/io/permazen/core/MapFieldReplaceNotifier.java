
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class MapFieldReplaceNotifier<K, V> extends MapFieldNotifier<K, V> {

    final K key;
    final V oldValue;
    final V newValue;

    MapFieldReplaceNotifier(MapField<K, V> field, ObjId id, K key, V oldValue, V newValue) {
        super(field, id);
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onMapFieldReplace(tx, this.id, this.field, path, referrers, this.key, this.oldValue, this.newValue);
    }
}
