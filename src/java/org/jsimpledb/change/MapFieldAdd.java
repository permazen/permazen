
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

import java.util.Map;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ObjId;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a new key/value pair is added to a map field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <K> the type of the changed map's key
 * @param <V> the type of the changed map's value
 */
public class MapFieldAdd<T, K, V> extends MapFieldChange<T> {

    private final K key;
    private final V value;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the map field that changed
     * @param storageId the storage ID of the affected field
     * @param fieldName the name of the field that changed
     * @param key the key of the new key/value pair
     * @param value the value of the new key/value pair
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public MapFieldAdd(T jobj, int storageId, String fieldName, K key, V value) {
        super(jobj, storageId, fieldName);
        this.key = key;
        this.value = value;
    }

    @Override
    public <R> R visit(FieldChangeSwitch<R> target) {
        return target.caseMapFieldAdd(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(JTransaction tx, ObjId id) {
        ((Map<K, V>)tx.readMapField(id, this.getStorageId())).put(this.key, this.value);
    }

    /**
     * Get the key of the new key/value pair that was added.
     */
    public K getKey() {
        return this.key;
    }

    /**
     * Get the value of the new key/value pair that was added.
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
        final MapFieldAdd<?, ?, ?> that = (MapFieldAdd<?, ?, ?>)obj;
        return (this.key != null ? this.key.equals(that.key) : that.key == null)
          && (this.value != null ? this.value.equals(that.value) : that.value == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.key != null ? this.key.hashCode() : 0) ^ (this.value != null ? this.value.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "MapFieldAdd[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",key="
          + this.key + ",value=" + this.value + "]";
    }
}

