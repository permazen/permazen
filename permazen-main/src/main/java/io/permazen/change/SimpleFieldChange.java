
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.annotation.OnChange;

import java.util.Objects;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods when a simple field changes.
 *
 * @param <T> the type of the object containing the changed field
 * @param <V> the type of the changed field
 */
public class SimpleFieldChange<T, V> extends FieldChange<T> {

    private final V oldValue;
    private final V newValue;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the field that changed
     * @param fieldName the name of the field that changed
     * @param oldValue the old field value
     * @param newValue the new field value
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public SimpleFieldChange(T jobj, String fieldName, V oldValue, V newValue) {
        super(jobj, fieldName);
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseSimpleFieldChange(this);
    }

    @Override
    public void apply(PermazenTransaction jtx, PermazenObject jobj) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        jtx.writeSimpleField(jobj, this.getFieldName(), this.newValue, false);
    }

    /**
     * Get the value of the field before the change.
     *
     * @return the old field value
     */
    public V getOldValue() {
        return this.oldValue;
    }

    /**
     * Get the value of the field after the change.
     *
     * @return the new field value
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
        final SimpleFieldChange<?, ?> that = (SimpleFieldChange<?, ?>)obj;
        return Objects.equals(this.oldValue, that.oldValue)
          && Objects.equals(this.newValue, that.newValue);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ Objects.hashCode(this.oldValue)
          ^ Objects.hashCode(this.newValue);
    }

    @Override
    public String toString() {
        return "SimpleFieldChange[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",oldValue="
          + this.oldValue + ",newValue=" + this.newValue + "]";
    }
}
