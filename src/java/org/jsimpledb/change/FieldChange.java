
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ObjId;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when a field changes.
 *
 * <p>
 * Note that it's possible, using the core API, to change a field without first updating the containing object's schema version.
 * As older schema versions may have different fields than the schema version associated with a particular
 * {@link org.jsimpledb.JSimpleDB} instance, it's therefore possible to receive change notifications about changes to fields
 * not present in the current schema. This will not happen unless the lower level core API is used directly, {@link FieldChange}
 * events are being generated manually, etc.
 * </p>
 *
 * @param <T> the type of the object containing the changed field
 */
public abstract class FieldChange<T> {

    private final T jobj;
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
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        if (storageId <= 0)
            throw new IllegalArgumentException("storageId <= 0");
        if (fieldName == null)
            throw new IllegalArgumentException("null fieldName");
        this.jobj = jobj;
        this.storageId = storageId;
        this.fieldName = fieldName;
    }

    /**
     * Get the Java model object containing the field that changed.
     *
     * <p>
     * Although not declared as such to allow flexibility in Java model types, the returned object
     * will always be a {@link JObject} instance.
     * </p>
     */
    public T getObject() {
        return this.jobj;
    }

    /**
     * Get the storage ID of the field that changed.
     *
     * @return chagned field's storage ID
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

    /**
     * Apply visitor pattern. Invokes the method of {@code target} corresponding to this instance's type.
     *
     * @param target visitor pattern target
     * @return value returned by the selected method of {@code target}
     */
    public abstract <R> R visit(FieldChangeSwitch<R> target);

    /**
     * Apply this change to the given object in the given transaction.
     *
     * @param id the ID of the target object to which to apply this change
     * @param tx the transaction in which to apply this change
     * @throws IllegalArgumentException if {@code tx} is null
     * @throws org.jsimpledb.core.DeletedObjectException if no object with ID {@code id} exists in {@code tx}
     * @throws org.jsimpledb.core.UnknownFieldException  if the target object in {@code tx} has a schema version that
     *  does not contain the affected field, or in which the affected field has a different type
     * @throws RuntimeException if there is some other incompatibility between this change and the target object,
     *  for example, setting a list element at an index that is out of bounds
     * @throws StaleTransactionException if {@code tx} is no longer usable
     */
    public abstract void apply(JTransaction tx, ObjId id);

    /**
     * Apply this change to the object associated with this instance in the transaction associated with the current thread.
     *
     * <p>
     * This is a convenience method, equivalent to:
     *  <blockquote><code>
     *  apply(tx, ((JObject)this.getObject()).getObjId());
     *  </code></blockquote>
     * </p>
     *
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public void apply(JTransaction tx) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        this.apply(tx, ((JObject)this.jobj).getObjId());
    }

    /**
     * Apply this change to the transaction associated with the current thread.
     *
     * <p>
     * This is a convenience method, equivalent to:
     *  <blockquote><code>
     *  apply(JTransaction.getCurrent())
     *  </code></blockquote>
     * </p>
     *
     * @throws IllegalStateException if there is no {@link JTransaction} associated with the current thread
     */
    public void apply() {
        this.apply(JTransaction.getCurrent());
    }

    /**
     * Apply this change to the specified object.
     *
     * <p>
     * This is a convenience method, equivalent to:
     *  <blockquote><code>
     *  apply(obj.getTransaction(), jobj.getObjId());
     *  </code></blockquote>
     * </p>
     *
     * @throws IllegalStateException if there is no {@link JTransaction} associated with {@code jobj} or the current thread
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public void apply(JObject jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        this.apply(jobj.getTransaction(), jobj.getObjId());
    }

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

