
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a map field changes.
 *
 * @param <T> the type of the object containing the changed field
 * @param <K> the type of the changed map's key
 * @param <V> the type of the changed map's value
 */
public abstract class MapFieldChange<T, K, V> extends FieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the map field that changed
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    protected MapFieldChange(T jobj) {
        super(jobj);
    }
}

