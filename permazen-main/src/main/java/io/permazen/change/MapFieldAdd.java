
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.annotation.OnChange;

import java.util.Map;
import java.util.Objects;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods
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
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseMapFieldAdd(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(JTransaction jtx, JObject jobj) {
        ((Map<K, V>)jtx.readMapField(jobj.getObjId(), this.getStorageId(), false)).put(this.key, this.value);
    }

    /**
     * Get the key of the new key/value pair that was added.
     *
     * @return the key of the newly added key/value pair
     */
    public K getKey() {
        return this.key;
    }

    /**
     * Get the value of the new key/value pair that was added.
     *
     * @return the value of the newly added key/value pair
     */
    public V getValue() {
        return this.value;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapFieldAdd<?, ?, ?> that = (MapFieldAdd<?, ?, ?>)obj;
        return Objects.equals(this.key, that.key)
          && Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.key) ^ Objects.hashCode(this.value);
    }

    @Override
    public String toString() {
        return "MapFieldAdd[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",key="
          + this.key + ",value=" + this.value + "]";
    }
}
