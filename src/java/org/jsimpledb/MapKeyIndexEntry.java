
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Returned by queries into the indexes assocated with map field keys.
 *
 * @see org.jsimpledb.annotation.IndexQuery
 *
 * @param <T> Java type of the object containing the map field
 * @param <V> map field value type
 */
public class MapKeyIndexEntry<T, V> extends MapIndexEntry<T, V> {

    MapKeyIndexEntry(T obj, V value) {
        super(obj, value);
    }

    /**
     * Get the value associated with the map entry represented by this instance.
     *
     * @return map entry value, possibly null
     */
    public V getValue() {
        return this.other;
    }
}

