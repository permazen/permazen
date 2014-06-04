
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ObjId;

/**
 * Change notification that indicates a new object has been created.
 *
 * @param <T> the type of the object that was created
 */
public class ObjectCreate<T> extends Change<T> {

    /**
     * Constructor.
     *
     * @param jobj Java model object that was created
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public ObjectCreate(T jobj) {
        super(jobj);
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseObjectCreate(this);
    }

    @Override
    public void apply(JTransaction tx, ObjId id) {
        tx.recreate(id);
    }

// Object

    @Override
    public String toString() {
        return "ObjectCreate[object=" + this.getObject() + "]";
    }
}

