
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a map field is cleared.
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
    public String toString() {
        return "MapFieldClear[object=" + this.getObject() + "]";
    }
}

