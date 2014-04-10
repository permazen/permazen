
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import com.google.common.base.Converter;

class MapValueIndexEntryConverter<T, K, WK>
  extends Converter<MapValueIndexEntry<T, K>, org.jsimpledb.MapValueIndexEntry<WK>> {

    private final ReferenceConverter referenceConverter;
    private final Converter<K, WK> keyConverter;

    MapValueIndexEntryConverter(ReferenceConverter referenceConverter, Converter<K, WK> keyConverter) {
        this.referenceConverter = referenceConverter;
        this.keyConverter = keyConverter;
    }

    @Override
    protected org.jsimpledb.MapValueIndexEntry<WK> doForward(MapValueIndexEntry<T, K> entry) {
        if (entry == null)
            return null;
        return new org.jsimpledb.MapValueIndexEntry<WK>(this.referenceConverter.convert((JObject)entry.getObject()),
          this.keyConverter.convert(entry.getKey()));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MapValueIndexEntry<T, K> doBackward(org.jsimpledb.MapValueIndexEntry<WK> entry) {
        if (entry == null)
            return null;
        return new MapValueIndexEntry<T, K>((T)this.referenceConverter.reverse().convert(entry.getObjId()),
          this.keyConverter.reverse().convert(entry.getKey()));
    }
}

