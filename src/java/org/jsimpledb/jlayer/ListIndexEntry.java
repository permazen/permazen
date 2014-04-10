
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

/**
 * Returned by queries into the indexes assocated with list fields.
 *
 * @see org.jsimpledb.annotation.IndexQuery
 *
 * @param <T> Java type of the object containing the list field
 */
public class ListIndexEntry<T> {

    final T jobj;
    final int index;

    public ListIndexEntry(T obj, int index) {
        if (!(obj instanceof JObject))
            throw new IllegalArgumentException("obj is not a JObject: " + obj);
        if (index < 0)
            throw new IllegalArgumentException("index < 0");
        this.jobj = obj;
        this.index = index;
    }

    /**
     * Get the object whose list field contains the indexed value.
     */
    public T getObject() {
        return this.jobj;
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
        return ((JObject)this.jobj).getObjId().hashCode() ^ this.index;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ListIndexEntry<?> that = (ListIndexEntry<?>)obj;
        return ((JObject)this.jobj).getObjId().equals(((JObject)that.jobj).getObjId()) && this.index == that.index;
    }
}

