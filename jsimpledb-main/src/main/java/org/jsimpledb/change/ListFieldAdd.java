
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.change;

import java.util.List;

import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when an element is added to a list field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed list's elements
 */
public class ListFieldAdd<T, E> extends ListFieldChange<T> {

    private final int index;
    private final E element;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the list field that changed
     * @param storageId the storage ID of the affected field
     * @param fieldName the name of the field that changed
     * @param index index at which the addition occurred
     * @param element the element that was added
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public ListFieldAdd(T jobj, int storageId, String fieldName, int index, E element) {
        super(jobj, storageId, fieldName);
        this.index = index;
        this.element = element;
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseListFieldAdd(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(JTransaction jtx, JObject jobj) {
        ((List<E>)jtx.readListField(jobj.getObjId(), this.getStorageId(), false)).add(this.index, this.element);
    }

    /**
     * Get the list index at which the new element was added.
     *
     * @return the index at which the value was added
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * Get the element that was added to the list.
     *
     * @return the value added to the list
     */
    public E getElement() {
        return this.element;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ListFieldAdd<?, ?> that = (ListFieldAdd<?, ?>)obj;
        return this.index == that.index && (this.element != null ? this.element.equals(that.element) : that.element == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.index ^ (this.element != null ? this.element.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "ListFieldAdd[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",index="
          + this.index + ",element=" + this.element + "]";
    }
}

