
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Describes whether and how objects are validated within a {@link JTransaction}.
 */
public enum ValidationMode {

    /**
     * Validation is disabled. No objects will be enqueued for validation even if {@link JObject#revalidate} is invoked.
     */
    DISABLED,

    /**
     * Objects are enqueued for validation only when {@link JObject#revalidate} is explicitly invoked.
     */
    MANUAL,

    /**
     * Objects are enqueued for validation when {@link JObject#revalidate} is invoked, or automatically
     * whenever any field in the object having one or more JSR 303 annotation constraints changes.
     */
    AUTOMATIC;
}

