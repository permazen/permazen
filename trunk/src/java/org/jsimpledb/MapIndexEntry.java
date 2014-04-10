
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Support superclass for {@link MapField} key and value index entry classes.
 *
 * @param <T> Java type of the object containing the list field
 * @param <V> the other map sub-field type
 */
abstract class MapIndexEntry<T, V> {

    final T jobj;
    final V other;

    public MapIndexEntry(T obj, V other) {
        if (!(obj instanceof JObject))
            throw new IllegalArgumentException("obj is not a JObject: " + obj);
        this.jobj = obj;
        this.other = other;
    }

    /**
     * Get the object whose map field contains the indexed value.
     */
    public T getObject() {
        return this.jobj;
    }

// Object

    @Override
    public int hashCode() {
        return ((JObject)this.jobj).getObjId().hashCode() ^ (this.other != null ? this.other.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final MapIndexEntry<?, ?> that = (MapIndexEntry<?, ?>)obj;
        return ((JObject)this.jobj).getObjId().equals(((JObject)that.jobj).getObjId())
          && (this.other != null ? this.other.equals(that.other) : that.other == null);
    }
}

