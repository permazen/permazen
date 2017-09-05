
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

/**
 * Notification object that gets passed to {@link io.permazen.annotation.OnChange &#64;OnChange}-annotated methods
 * when a field changes.
 *
 * <p>
 * Note that it's possible, using the core API, to change a field without first updating the containing object's schema version.
 * As older schema versions may have different fields than the schema version associated with a particular
 * {@link io.permazen.JSimpleDB} instance, it's therefore possible to receive change notifications about changes to fields
 * not present in the current schema. This will not happen unless the lower level core API is used directly, {@link FieldChange}
 * events are being generated manually, etc.
 *
 * @param <T> the type of the object containing the changed field
 */
public abstract class FieldChange<T> extends Change<T> {

    private final int storageId;
    private final String fieldName;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the field that changed
     * @param storageId the storage ID of the affected field
     * @param fieldName the name of the field that changed
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    protected FieldChange(T jobj, int storageId, String fieldName) {
        super(jobj);
        Preconditions.checkArgument(storageId > 0, "storageId <= 0");
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        this.storageId = storageId;
        this.fieldName = fieldName;
    }

    /**
     * Get the storage ID of the field that changed.
     *
     * @return changed field's storage ID
     */
    public int getStorageId() {
        return this.storageId;
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
        return this.storageId == that.storageId && this.fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.storageId ^ this.fieldName.hashCode();
    }
}

