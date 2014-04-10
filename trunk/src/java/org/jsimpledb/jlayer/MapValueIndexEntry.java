
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

/**
 * Returned by queries into the indexes assocated with map field values.
 *
 * @see org.jsimpledb.annotation.IndexQuery
 *
 * @param <T> Java type of the object containing the map field
 * @param <K> map field key type
 */
public class MapValueIndexEntry<T, K> extends MapIndexEntry<T, K> {

    MapValueIndexEntry(T obj, K key) {
        super(obj, key);
    }

    /**
     * Get the key associated with the map entry represented by this instance.
     *
     * @return map entry key, possibly null
     */
    public K getKey() {
        return this.other;
    }
}

