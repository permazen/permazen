
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
 * @param <K> the type of the changed map's key
 * @param <V> the type of the changed map's value
 */
public class MapFieldClear<T, K, V> extends MapFieldChange<T, K, V> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the map field that was cleared
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public MapFieldClear(T jobj) {
        super(jobj);
    }

    @Override
    public String toString() {
        return "MapFieldClear[object=" + this.getObject() + "]";
    }
}

