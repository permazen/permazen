
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Returned by queries into the indexes assocated with {@link ListField}s.
 *
 * @see Transaction#queryListFieldEntries
 */
public class ListIndexEntry {

    final ObjId id;
    final int index;

    public ListIndexEntry(ObjId id, int index) {
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (index < 0)
            throw new IllegalArgumentException("index < 0");
        this.id = id;
        this.index = index;
    }

    /**
     * Get the object ID of the object whose {@link ListField} contains the indexed value.
     *
     * @return object ID, never null
     */
    public ObjId getObjId() {
        return this.id;
    }

    /**
     * Get the index of the queried value in the list.
     *
     * @return zero-based list index
     */
    public int getIndex() {
        return this.index;
    }

// Object

    @Override
    public int hashCode() {
        return this.id.hashCode() ^ this.index;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ListIndexEntry that = (ListIndexEntry)obj;
        return this.id.equals(that.id) && this.index == that.index;
    }
}

