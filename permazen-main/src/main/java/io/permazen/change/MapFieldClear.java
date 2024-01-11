
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.annotation.OnChange;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods when a map field is cleared.
 *
 * @param <T> the type of the object containing the changed field
 */
public class MapFieldClear<T> extends MapFieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the map field that was cleared
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public MapFieldClear(T jobj, String fieldName) {
        super(jobj, fieldName);
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseMapFieldClear(this);
    }

    @Override
    public void apply(PermazenTransaction jtx, PermazenObject jobj) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        Preconditions.checkArgument(jobj != null, "null jobj");
        jtx.readMapField(jobj.getObjId(), this.getFieldName(), false).clear();
    }

// Object

    @Override
    public String toString() {
        return "MapFieldClear[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\"]";
    }
}
