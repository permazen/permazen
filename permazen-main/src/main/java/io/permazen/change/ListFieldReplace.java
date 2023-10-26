
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.annotation.OnChange;

import java.util.List;
import java.util.Objects;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods
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
        ((List<E>)jtx.readListField(jobj.getObjId(), this.getStorageId(), false)).set(this.index, this.newValue);
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
          && Objects.equals(this.oldValue, that.oldValue)
          && Objects.equals(this.newValue, that.newValue);
    }

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ this.index
          ^ Objects.hashCode(this.oldValue)
          ^ Objects.hashCode(this.newValue);
    }

    @Override
    public String toString() {
        return "ListFieldReplace[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",index=" + this.index
          + ",oldValue=" + this.oldValue + ",newValue=" + this.newValue + "]";
    }
}
