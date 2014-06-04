
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when an element is removed from a list field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed list's elements
 */
public class ListFieldRemove<T, E> extends ListFieldChange<T> {

    private final int index;
    private final E element;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the list field that changed
     * @param fieldName the name of the field that changed
     * @param index index at which the removal occurred
     * @param element the element that was removed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public ListFieldRemove(T jobj, String fieldName, int index, E element) {
        super(jobj, fieldName);
        this.index = index;
        this.element = element;
    }

    @Override
    public <R> R visit(FieldChangeSwitch<R> target) {
        return target.caseListFieldRemove(this);
    }

    /**
     * Get the list index from which the element was removed.
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * Get the element that was removed from the list.
     */
    public E getElement() {
        return this.element;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ListFieldRemove<?, ?> that = (ListFieldRemove<?, ?>)obj;
        return this.index == that.index && (this.element != null ? this.element.equals(that.element) : that.element == null);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.index ^ (this.element != null ? this.element.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "ListFieldRemove[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",index="
          + this.index + ",element=" + this.element + "]";
    }
}

