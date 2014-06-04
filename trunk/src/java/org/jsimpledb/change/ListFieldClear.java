
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a list field is cleared.
 *
 * @param <T> the type of the object containing the changed field
 */
public class ListFieldClear<T> extends ListFieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the list field that was cleared
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public ListFieldClear(T jobj, String fieldName) {
        super(jobj, fieldName);
    }

    @Override
    public <R> R visit(FieldChangeSwitch<R> target) {
        return target.caseListFieldClear(this);
    }

    @Override
    public String toString() {
        return "ListFieldClear[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\"]";
    }
}

