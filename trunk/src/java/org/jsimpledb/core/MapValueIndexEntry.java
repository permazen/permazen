
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Item returned by queries into the indexes assocated with {@link MapField} values.
 *
 * @see Transaction#queryMapFieldValueEntries
 * @param <K> map field key type
 */
public class MapValueIndexEntry<K> extends MapIndexEntry<K> {

    /**
     * Constructor.
     */
    public MapValueIndexEntry(ObjId id, K key) {
        super(id, key);
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

