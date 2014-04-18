
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
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public ListFieldClear(T jobj) {
        super(jobj);
    }

    @Override
    public String toString() {
        return "ListFieldClear[object=" + this.getObject() + "]";
    }
}

