
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a set field is cleared.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed set's elements
 */
public class SetFieldClear<T, E> extends SetFieldChange<T, E> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the set field that was cleared
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public SetFieldClear(T jobj) {
        super(jobj);
    }

    @Override
    public String toString() {
        return "SetFieldClear[object=" + this.getObject() + "]";
    }
}

