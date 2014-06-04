
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a map field changes.
 *
 * @param <T> the type of the object containing the changed field
 */
public abstract class MapFieldChange<T> extends FieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the map field that changed
     * @param storageId the storage ID of the affected field
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    protected MapFieldChange(T jobj, int storageId, String fieldName) {
        super(jobj, storageId, fieldName);
    }
}

