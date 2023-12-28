
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.annotation.OnChange;

import java.util.Map;
import java.util.Objects;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods
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
     * @param fieldName the name of the field that changed
     * @param key the key whose value was changed
     * @param oldValue the old value associated with {@code key}
     * @param newValue the new value associated with {@code key}
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public MapFieldReplace(T jobj, String fieldName, K key, V oldValue, V newValue) {
        super(jobj, fieldName);
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseMapFieldReplace(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(JTransaction jtx, JObject jobj) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        Preconditions.checkArgument(jobj != null, "null jobj");
        ((Map<K, V>)jtx.readMapField(jobj.getObjId(), this.getFieldName(), false)).put(this.key, this.newValue);
    }

    /**
     * Get the key of the key/value pair whose value was replaced.
     *
     * @return the key of the replaced value
     */
    public K getKey() {
        return this.key;
    }

    /**
     * Get the value of the key/value pair before the change.
     *
     * @return the replaced value
     */
    public V getOldValue() {
        return this.oldValue;
    }

    /**
     * Get the value of the key/value pair after the change.
     *
     * @return the replacing value
     */
    public V getNewValue() {
        return this.newValue;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapFieldReplace<?, ?, ?> that = (MapFieldReplace<?, ?, ?>)obj;
        return Objects.equals(this.key, that.key)
          && Objects.equals(this.oldValue, that.oldValue)
          && Objects.equals(this.newValue, that.newValue);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.key) ^ Objects.hashCode(this.oldValue) ^ Objects.hashCode(this.newValue);
    }

    @Override
    public String toString() {
        return "MapFieldReplace[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",key=" + this.key
          + ",oldValue=" + this.oldValue + ",newValue=" + this.newValue + "]";
    }
}
