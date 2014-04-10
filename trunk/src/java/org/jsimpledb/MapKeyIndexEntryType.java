
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/**
 * {@link FieldType} of a map key index entry.
 *
 * @param <V> map value field type
 */
class MapKeyIndexEntryType<V> extends MapIndexEntryType<MapKeyIndexEntry<V>, V> {

    @SuppressWarnings("serial")
    MapKeyIndexEntryType(FieldType<V> valueType) {
        super("MapKeyIndexEntry", new TypeToken<MapKeyIndexEntry<V>>() { }.where(
          new TypeParameter<V>() { }, valueType.typeToken.wrap()), valueType);
    }

    @Override
    protected MapKeyIndexEntry<V> createMapIndexEntry(ObjId id, V value) {
        return new MapKeyIndexEntry<V>(id, value);
    }
}

