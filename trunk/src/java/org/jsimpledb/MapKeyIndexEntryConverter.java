
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

class MapKeyIndexEntryConverter<T, V, WV> extends Converter<MapKeyIndexEntry<T, V>, org.jsimpledb.core.MapKeyIndexEntry<WV>> {

    private final ReferenceConverter referenceConverter;
    private final Converter<V, WV> valueConverter;

    MapKeyIndexEntryConverter(ReferenceConverter referenceConverter, Converter<V, WV> valueConverter) {
        this.referenceConverter = referenceConverter;
        this.valueConverter = valueConverter;
    }

    @Override
    protected org.jsimpledb.core.MapKeyIndexEntry<WV> doForward(MapKeyIndexEntry<T, V> entry) {
        if (entry == null)
            return null;
        return new org.jsimpledb.core.MapKeyIndexEntry<WV>(this.referenceConverter.convert((JObject)entry.getObject()),
          this.valueConverter.convert(entry.getValue()));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MapKeyIndexEntry<T, V> doBackward(org.jsimpledb.core.MapKeyIndexEntry<WV> entry) {
        if (entry == null)
            return null;
        return new MapKeyIndexEntry<T, V>((T)this.referenceConverter.reverse().convert(entry.getObjId()),
          this.valueConverter.reverse().convert(entry.getValue()));
    }
}

