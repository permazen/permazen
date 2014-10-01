
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

class MapValueIndexEntryConverter<T, K, WK> extends Converter<MapValueIndexEntry<T, K>, org.jsimpledb.core.MapValueIndexEntry<WK>> {

    private final ReferenceConverter referenceConverter;
    private final Converter<K, WK> keyConverter;

    MapValueIndexEntryConverter(ReferenceConverter referenceConverter, Converter<K, WK> keyConverter) {
        if (referenceConverter == null)
            throw new IllegalArgumentException("null referenceConverter");
        if (keyConverter == null)
            throw new IllegalArgumentException("null keyConverter");
        this.referenceConverter = referenceConverter;
        this.keyConverter = keyConverter;
    }

    @Override
    protected org.jsimpledb.core.MapValueIndexEntry<WK> doForward(MapValueIndexEntry<T, K> entry) {
        if (entry == null)
            return null;
        return new org.jsimpledb.core.MapValueIndexEntry<WK>(this.referenceConverter.convert((JObject)entry.getObject()),
          this.keyConverter.convert(entry.getKey()));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MapValueIndexEntry<T, K> doBackward(org.jsimpledb.core.MapValueIndexEntry<WK> entry) {
        if (entry == null)
            return null;
        return new MapValueIndexEntry<T, K>((T)this.referenceConverter.reverse().convert(entry.getObjId()),
          this.keyConverter.reverse().convert(entry.getKey()));
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final MapValueIndexEntryConverter<?, ?, ?> that = (MapValueIndexEntryConverter<?, ?, ?>)obj;
        return this.referenceConverter.equals(that.referenceConverter) && this.keyConverter.equals(that.keyConverter);
    }

    @Override
    public int hashCode() {
        return this.referenceConverter.hashCode() ^ this.keyConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[referenceConverter=" + this.referenceConverter
          + ",keyConverter=" + this.keyConverter + "]";
    }
}

