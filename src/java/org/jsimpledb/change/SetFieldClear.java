
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.change;

import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a set field is cleared.
 *
 * @param <T> the type of the object containing the changed field
 */
public class SetFieldClear<T> extends SetFieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the set field that was cleared
     * @param storageId the storage ID of the affected field
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public SetFieldClear(T jobj, int storageId, String fieldName) {
        super(jobj, storageId, fieldName);
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseSetFieldClear(this);
    }

    @Override
    public void apply(JTransaction jtx, JObject jobj) {
        jtx.readSetField(jobj.getObjId(), this.getStorageId(), false).clear();
    }

// Object

    @Override
    public String toString() {
        return "SetFieldClear[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\"]";
    }
}

