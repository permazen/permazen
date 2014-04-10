
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a field changes.
 *
 * @param <T> the type of the object containing the changed field
 */
public abstract class FieldChange<T> {

    private final T jobj;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the field that changed
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    protected FieldChange(T jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        this.jobj = jobj;
    }

    /**
     * Get the Java model object containing the field that changed.
     */
    public T getObject() {
        return this.jobj;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final FieldChange<?> that = (FieldChange<?>)obj;
        return this.jobj.equals(that.jobj);
    }

    @Override
    public int hashCode() {
        return this.jobj.hashCode();
    }
}

