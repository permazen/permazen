
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a key/value pair is removed from a map field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <K> the type of the changed map's key
 * @param <V> the type of the changed map's value
 */
public class MapFieldRemove<T, K, V> extends MapFieldChange<T> {

    private final K key;
    private final V value;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the map field that changed
     * @param fieldName the name of the field that changed
     * @param key the key of the removed key/value pair
     * @param value the value of the removed key/value pair
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public MapFieldRemove(T jobj, String fieldName, K key, V value) {
        super(jobj, fieldName);
        this.key = key;
        this.value = value;
    }

    /**
     * Get the key of the key/value pair that was removed.
     */
    public K getKey() {
        return this.key;
    }

    /**
     * Get the value of the key/value pair that was removed.
     */
    public V getValue() {
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapFieldRemove<?, ?, ?> that = (MapFieldRemove<?, ?, ?>)obj;
        return (this.key != null ? this.key.equals(that.key) : that.key == null)
          && (this.value != null ? this.value.equals(that.value) : that.value == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.key != null ? this.key.hashCode() : 0) ^ (this.value != null ? this.value.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "MapFieldRemove[object=" + this.getObject() + ",key=" + this.key + ",value=" + this.value + "]";
    }
}

