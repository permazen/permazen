
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
 * Object change notification.
 *
 * @param <T> the type of the object that changed
 */
public abstract class Change<T> {

    private final T jobj;

    /**
     * Constructor.
     *
     * @param jobj Java model object that changed
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    protected Change(T jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        this.jobj = jobj;
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
     * Get the Java model object containing the field that changed.
     *
     * <p>
     * This is a convenience method, equivalent to:
     *  <blockquote><code>
     *  (JObject)getObject()
     *  </code></blockquote>
     * </p>
     */
    public JObject getJObject() {
        return (JObject)this.jobj;
    }

    /**
     * Apply visitor pattern. Invokes the method of {@code target} corresponding to this instance's type.
     *
     * @param target visitor pattern target
     * @return value returned by the selected method of {@code target}
     */
    public abstract <R> R visit(ChangeSwitch<R> target);

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
     * Apply this change to the object associated with this instance in the given transaction.
     *
     * <p>
     * This is a convenience method, equivalent to:
     *  <blockquote><code>
     *  apply(tx, this.getJObject().getObjId());
     *  </code></blockquote>
     * </p>
     *
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public void apply(JTransaction tx) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        this.apply(tx, this.getJObject().getObjId());
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
     * @throws IllegalStateException if there is no {@link JTransaction} associated with {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public void apply(JObject jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        this.apply(jobj.getTransaction(), jobj.getObjId());
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Change<?> that = (Change<?>)obj;
        return this.jobj.equals(that.jobj);
    }

    @Override
    public int hashCode() {
        return this.jobj.hashCode();
    }
}

