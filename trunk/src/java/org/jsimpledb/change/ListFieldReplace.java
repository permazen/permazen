
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

import java.util.List;

import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when an element is replaced in a list field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed list's elements
 */
public class ListFieldReplace<T, E> extends ListFieldChange<T> {

    private final int index;
    private final E oldValue;
    private final E newValue;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the list field that changed
     * @param storageId the storage ID of the affected field
     * @param fieldName the name of the field that changed
     * @param index the index at which the replacement occurred
     * @param oldValue the old value in the list
     * @param newValue the new value in the list
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public ListFieldReplace(T jobj, int storageId, String fieldName, int index, E oldValue, E newValue) {
        super(jobj, storageId, fieldName);
        this.index = index;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseListFieldReplace(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(JTransaction jtx, JObject jobj) {
        ((List<E>)jtx.readListField(jobj, this.getStorageId(), false)).set(this.index, this.newValue);
    }

    /**
     * Get the list index at which the element was replaced.
     *
     * @return the index at which the value was replaced
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * Get the value of the field before the change.
     *
     * @return the replaced value
     */
    public E getOldValue() {
        return this.oldValue;
    }

    /**
     * Get the value of the field after the change.
     *
     * @return the replacing value
     */
    public E getNewValue() {
        return this.newValue;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ListFieldReplace<?, ?> that = (ListFieldReplace<?, ?>)obj;
        return this.index == that.index
          && (this.oldValue != null ? this.oldValue.equals(that.oldValue) : that.oldValue == null)
          && (this.newValue != null ? this.newValue.equals(that.newValue) : that.newValue == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ this.index
          ^ (this.oldValue != null ? this.oldValue.hashCode() : 0)
          ^ (this.newValue != null ? this.newValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "ListFieldReplace[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",index=" + this.index
          + ",oldValue=" + this.oldValue + ",newValue=" + this.newValue + "]";
    }
}

