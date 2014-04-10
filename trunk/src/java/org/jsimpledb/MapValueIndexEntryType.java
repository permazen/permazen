
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/**
 * {@link FieldType} of a map value index entry.
 *
 * @param <K> map key field type
 */
class MapValueIndexEntryType<K> extends MapIndexEntryType<MapValueIndexEntry<K>, K> {

    @SuppressWarnings("serial")
    MapValueIndexEntryType(FieldType<K> keyType) {
        super("MapValueIndexEntry", new TypeToken<MapValueIndexEntry<K>>() { }.where(
          new TypeParameter<K>() { }, keyType.typeToken.wrap()), keyType);
    }

    @Override
    protected MapValueIndexEntry<K> createMapIndexEntry(ObjId id, K key) {
        return new MapValueIndexEntry<K>(id, key);
    }
}

