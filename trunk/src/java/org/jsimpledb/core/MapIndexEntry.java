
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Support superclass for {@link MapField} key and value index entry classes.
 *
 * @param <T> un-indexed map sub-field type
 */
abstract class MapIndexEntry<T> {

    final ObjId id;
    final T other;

    protected MapIndexEntry(ObjId id, T other) {
        if (id == null)
            throw new IllegalArgumentException("null id");
        this.id = id;
        this.other = other;
    }

    /**
     * Get the object ID of the object whose {@link MapField} contains the indexed value.
     *
     * @return object ID, never null
     */
    public ObjId getObjId() {
        return this.id;
    }

// Object

    @Override
    public int hashCode() {
        return this.id.hashCode() ^ (this.other != null ? this.other.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final MapIndexEntry<?> that = (MapIndexEntry)obj;
        return this.id.equals(that.id) && (this.other != null ? this.other.equals(that.other) : that.other == null);
    }
}

