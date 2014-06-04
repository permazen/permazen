
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a field changes.
 *
 * @param <T> the type of the object containing the changed field
 */
public abstract class FieldChange<T> {

    private final T jobj;
    private final String fieldName;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the field that changed
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    protected FieldChange(T jobj, String fieldName) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        if (fieldName == null)
            throw new IllegalArgumentException("null fieldName");
        this.jobj = jobj;
        this.fieldName = fieldName;
    }

    /**
     * Get the Java model object containing the field that changed.
     */
    public T getObject() {
        return this.jobj;
    }

    /**
     * Get the name of the field that changed.
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * Apply visitor pattern. Invokes the method of {@code target} corresponding to this instance's type.
     *
     * @param target visitor pattern target
     * @return value returned by the selected method of {@code target}
     */
    public abstract <R> R visit(FieldChangeSwitch<R> target);

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final FieldChange<?> that = (FieldChange<?>)obj;
        return this.jobj.equals(that.jobj) && this.fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
        return this.jobj.hashCode() ^ this.fieldName.hashCode();
    }
}

