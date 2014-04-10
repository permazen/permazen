
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Item returned by queries into the indexes assocated with {@link MapField} keys.
 *
 * @see Transaction#queryMapFieldKeyEntries
 * @param <V> map field value type
 */
public class MapKeyIndexEntry<V> extends MapIndexEntry<V> {

    /**
     * Constructor.
     */
    public MapKeyIndexEntry(ObjId id, V value) {
        super(id, value);
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

