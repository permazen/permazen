
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a list field changes.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed list's elements
 */
public abstract class ListFieldChange<T, E> extends FieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the list field that changed
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    protected ListFieldChange(T jobj) {
        super(jobj);
    }
}

