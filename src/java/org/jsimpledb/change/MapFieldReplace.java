
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when the value in a key/value pair is replaced with a new value.
 *
 * @param <T> the type of the object containing the changed field
 * @param <K> the type of the changed map's key
 * @param <V> the type of the changed map's value
 */
public class MapFieldReplace<T, K, V> extends MapFieldChange<T> {

    private final K key;
    private final V oldValue;
    private final V newValue;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the map field that changed
     * @param key the key whose value was changed
     * @param oldValue the old value associated with {@code key}
     * @param newValue the new value associated with {@code key}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public MapFieldReplace(T jobj, K key, V oldValue, V newValue) {
        super(jobj);
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    /**
     * Get the key of the key/value pair whose value was replaced.
     */
    public K getKey() {
        return this.key;
    }

    /**
     * Get the value of the key/value pair before the change.
     */
    public V getOldValue() {
        return this.oldValue;
    }

    /**
     * Get the value of the key/value pair after the change.
     */
    public V getNewValue() {
        return this.newValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapFieldReplace<?, ?, ?> that = (MapFieldReplace<?, ?, ?>)obj;
        return (this.key != null ? this.key.equals(that.key) : that.key == null)
          && (this.oldValue != null ? this.oldValue.equals(that.oldValue) : that.oldValue == null)
          && (this.newValue != null ? this.newValue.equals(that.newValue) : that.newValue == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ (this.key != null ? this.key.hashCode() : 0)
          ^ (this.oldValue != null ? this.oldValue.hashCode() : 0)
          ^ (this.newValue != null ? this.newValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "MapFieldReplace[object=" + this.getObject() + ",key=" + this.key
          + ",oldValue=" + this.oldValue + ",newValue=" + this.newValue + "]";
    }
}

