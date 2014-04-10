
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a simple field changes.
 *
 * @param <T> the type of the object containing the changed field
 * @param <F> the type of the changed field
 */
public class SimpleFieldChange<T, F> extends FieldChange<T> {

    private final F oldValue;
    private final F newValue;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the field that changed
     * @param oldValue the old field value
     * @param newValue the new field value
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public SimpleFieldChange(T jobj, F oldValue, F newValue) {
        super(jobj);
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    /**
     * Get the value of the field before the change.
     */
    public F getOldValue() {
        return this.oldValue;
    }

    /**
     * Get the value of the field after the change.
     */
    public F getNewValue() {
        return this.newValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleFieldChange<?, ?> that = (SimpleFieldChange<?, ?>)obj;
        return (this.oldValue != null ? this.oldValue.equals(that.oldValue) : that.oldValue == null)
          && (this.newValue != null ? this.newValue.equals(that.newValue) : that.newValue == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ (this.oldValue != null ? this.oldValue.hashCode() : 0)
          ^ (this.newValue != null ? this.newValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "SimpleFieldChange[object=" + this.getObject()
          + ",oldValue=" + this.oldValue + ",newValue=" + this.newValue + "]";
    }
}

