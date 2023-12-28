
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.annotation.OnChange;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods when a field changes.
 *
 * <p>
 * Note that it's possible, using the core API, to change a field without first updating the containing object's schema.
 * As older schemas may have different fields than the schema associated with a particular
 * {@link io.permazen.Permazen} instance, it's therefore possible to receive change notifications about changes to fields
 * not present in the current schema. This will not happen unless the lower level core API is used directly, {@link FieldChange}
 * events are being generated manually, etc.
 *
 * @param <T> the type of the object containing the changed field
 */
public abstract class FieldChange<T> extends Change<T> {

    private final String fieldName;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the field that changed
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    protected FieldChange(T jobj, String fieldName) {
        super(jobj);
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        this.fieldName = fieldName;
    }

    /**
     * Get the name of the field that changed.
     *
     * @return the name of the field that changed
     */
    public String getFieldName() {
        return this.fieldName;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final FieldChange<?> that = (FieldChange<?>)obj;
        return this.fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.fieldName.hashCode();
    }
}
