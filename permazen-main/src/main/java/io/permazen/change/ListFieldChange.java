
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import io.permazen.annotation.OnChange;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods when a list field changes.
 *
 * @param <T> the type of the object containing the changed field
 */
public abstract class ListFieldChange<T> extends FieldChange<T> {

    /**
     * Constructor.
     *
     * @param jobj Java object containing the list field that changed
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    protected ListFieldChange(T jobj, String fieldName) {
        super(jobj, fieldName);
    }
}
