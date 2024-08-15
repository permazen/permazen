
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

abstract class MapFieldNotifier<K, V> extends FieldChangeNotifier<MapFieldChangeListener, MapField<K, V>> {

    MapFieldNotifier(MapField<K, V> field, ObjId id) {
        super(field, id);
    }

    @Override
    public Class<MapFieldChangeListener> getListenerType() {
        return MapFieldChangeListener.class;
    }
}
