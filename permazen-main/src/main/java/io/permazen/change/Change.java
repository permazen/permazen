
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;

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
        Preconditions.checkArgument(jobj != null, "null jobj");
        this.jobj = jobj;
    }

    /**
     * Get the Java model object containing the field that changed.
     *
     * <p>
     * Although not declared as such to allow flexibility in Java model types, the returned object
     * will always be a {@link PermazenObject} instance.
     *
     * @return the changed object
     */
    public T getObject() {
        return this.jobj;
    }

    /**
     * Get the Java model object containing the field that changed.
     *
     * <p>
     * This is a convenience method, equivalent to:
     * <blockquote><pre>
     * (PermazenObject)getObject()
     * </pre></blockquote>
     *
     * @return the changed object as a {@link PermazenObject}
     */
    public PermazenObject getJObject() {
        return (PermazenObject)this.jobj;
    }

    /**
     * Apply visitor pattern. Invokes the method of {@code target} corresponding to this instance's type.
     *
     * @param target visitor pattern target
     * @param <R> visitor return type
     * @return value returned by the selected method of {@code target}
     */
    public abstract <R> R visit(ChangeSwitch<R> target);

    /**
     * Apply this change to the given object in the given transaction.
     *
     * @param jobj the target object to which to apply this change
     * @param jtx the transaction in which to apply this change
     * @throws io.permazen.core.DeletedObjectException if {@code jobj} does not exist in {@code jtx}
     * @throws io.permazen.core.UnknownFieldException  if {@code jobj} has a schema that
     *  does not contain the affected field, or in which the affected field has a different type
     * @throws RuntimeException if there is some other incompatibility between this change and the target object,
     *  for example, setting a list element at an index that is out of bounds
     * @throws io.permazen.kv.StaleTransactionException if {@code jtx} is no longer usable
     * @throws IllegalArgumentException if {@code jtx} or {@code jobj} is null
     */
    public abstract void apply(PermazenTransaction jtx, PermazenObject jobj);

    /**
     * Apply this change to the object associated with this instance in the given transaction.
     *
     * <p>
     * This is a convenience method, equivalent to:
     * <blockquote><pre>
     * apply(jtx, this.getJObject());
     * </pre></blockquote>
     *
     * @param jtx transaction in which to apply this change
     * @throws IllegalArgumentException if {@code jtx} is null
     */
    public void apply(PermazenTransaction jtx) {
        this.apply(jtx, this.getJObject());
    }

    /**
     * Apply this change to the transaction associated with the current thread.
     *
     * <p>
     * This is a convenience method, equivalent to:
     * <blockquote><pre>
     * apply(PermazenTransaction.getCurrent())
     * </pre></blockquote>
     *
     * @throws IllegalStateException if there is no {@link PermazenTransaction} associated with the current thread
     */
    public void apply() {
        this.apply(PermazenTransaction.getCurrent());
    }

    /**
     * Apply this change to the specified object.
     *
     * <p>
     * This is a convenience method, equivalent to:
     * <blockquote><pre>
     * apply(obj.getTransaction(), jobj);
     * </pre></blockquote>
     *
     * @param jobj object to which to apply this change
     * @throws IllegalStateException if there is no {@link PermazenTransaction} associated with {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public void apply(PermazenObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        this.apply(jobj.getTransaction(), jobj);
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
        return this.getClass().hashCode() ^ this.jobj.hashCode();
    }
}
