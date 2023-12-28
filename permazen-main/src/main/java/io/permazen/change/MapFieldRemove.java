
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.annotation.OnChange;

import java.util.Objects;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods
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

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseMapFieldRemove(this);
    }

    @Override
    public void apply(JTransaction jtx, JObject jobj) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        Preconditions.checkArgument(jobj != null, "null jobj");
        jtx.readMapField(jobj.getObjId(), this.getFieldName(), false).remove(this.key);
    }

    /**
     * Get the key of the key/value pair that was removed.
     *
     * @return the key of the removed key/value pair
     */
    public K getKey() {
        return this.key;
    }

    /**
     * Get the value of the key/value pair that was removed.
     *
     * @return the value of the removed key/value pair
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
        final MapFieldRemove<?, ?, ?> that = (MapFieldRemove<?, ?, ?>)obj;
        return Objects.equals(this.key, that.key)
          && Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.key) ^ Objects.hashCode(this.value);
    }

    @Override
    public String toString() {
        return "MapFieldRemove[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",key="
          + this.key + ",value=" + this.value + "]";
    }
}
