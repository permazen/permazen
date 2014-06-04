
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

import java.util.Set;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ObjId;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when an element is added to a set field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed set's elements
 */
public class SetFieldAdd<T, E> extends SetFieldChange<T> {

    private final E element;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the set field that changed
     * @param storageId the storage ID of the affected field
     * @param fieldName the name of the field that changed
     * @param element the element that was added
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public SetFieldAdd(T jobj, int storageId, String fieldName, E element) {
        super(jobj, storageId, fieldName);
        this.element = element;
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseSetFieldAdd(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(JTransaction tx, ObjId id) {
        ((Set<E>)tx.readSetField(id, this.getStorageId())).add(this.element);
    }

    /**
     * Get the element that was added to the set.
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
        final SetFieldAdd<?, ?> that = (SetFieldAdd<?, ?>)obj;
        return this.element != null ? this.element.equals(that.element) : that.element == null;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.element != null ? this.element.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "SetFieldAdd[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",element=" + this.element + "]";
    }
}

