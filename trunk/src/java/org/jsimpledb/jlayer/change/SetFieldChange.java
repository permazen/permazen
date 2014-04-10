
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a set field changes.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed set's elements
 */
public abstract class SetFieldChange<T, E> extends FieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the set field that changed
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    protected SetFieldChange(T jobj) {
        super(jobj);
    }
}

