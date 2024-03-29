
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.annotation.OnDelete;

/**
 * Change notification that indicates an object has been deleted.
 *
 * <p>
 * This type of change notification is never generated by Permazen itself; object deletion notifications are instead
 * delivered to {@link OnDelete &#64;OnDelete} methods, which do not take any parameters.
 * This class exists as a convenience for application code that may want to unify handling of
 * object change and object lifecycle events.
 *
 * @param <T> the type of the object that was deleted
 */
public class ObjectDelete<T> extends Change<T> {

    /**
     * Constructor.
     *
     * @param jobj Java model object that was deleted
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public ObjectDelete(T jobj) {
        super(jobj);
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseObjectDelete(this);
    }

    @Override
    public void apply(PermazenTransaction jtx, PermazenObject jobj) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        jtx.delete(jobj);
    }

// Object

    @Override
    public String toString() {
        return "ObjectDelete[objId=" + ((PermazenObject)this.getObject()).getObjId() + "]";
    }
}
