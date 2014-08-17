
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;

/**
 * Creates a new {@link Change} object based on an existing one where the {@link JObject}s referred to by the
 * new {@link Change} are copies in a different transaction of the originals. This is useful to allow database
 * change information to be accessed after the transaction in which the change occured has completed.
 */
public class ChangeCopier implements ChangeSwitch<Change<?>> {

    protected final JTransaction dest;
    protected final boolean copyRelated;

    /**
     * Primary constructor.
     *
     * @param dest destination transaction for copied {@link JObject}s
     * @param copyRelated whether to copy just the changed object, or also {@linkplain JObject#getRelatedObjects related objects}
     * @throws IllegalArgumentException if {@code dest} is null
     */
    public ChangeCopier(JTransaction dest, boolean copyRelated) {
        if (dest == null)
            throw new IllegalArgumentException("null dest");
        this.dest = dest;
        this.copyRelated = copyRelated;
    }

    /**
     * "Snapshot" constructor for when the destination transaction is the "snapshot" transaction of the transaction
     * associated with the current thread.
     *
     * <p>
     * This is a convenience constructor, and is equivalent to invoking:
     *  <blockquote><code>
     *  ChangeCopier(JTransaction.getCurrent().getSnapshotTransaction(), copyRelated)
     *  </code></blockquote>
     * </p>
     *
     * @throws IllegalStateException if this is not a snapshot instance and there is no {@link JTransaction}
     *  associated with the current thread
     */
    public ChangeCopier(boolean copyRelated) {
        this(JTransaction.getCurrent().getSnapshotTransaction(), copyRelated);
    }

    /**
     * Get the destination transaction configured in this instance.
     */
    public JTransaction getDestinationTransaction() {
        return this.dest;
    }

    /**
     * Determine whether this instance is configured to copy objects {@linkplain JObject#getRelatedObjects related}
     * to the changed object as well.
     */
    public boolean isCopyRelated() {
        return this.copyRelated;
    }

    @Override
    public <T> ObjectCreate<T> caseObjectCreate(ObjectCreate<T> change) {
        return new ObjectCreate<T>(this.copyIfReference(change.getObject()));
    }

    @Override
    public <T> ObjectDelete<T> caseObjectDelete(ObjectDelete<T> change) {
        return new ObjectDelete<T>(this.copyIfReference(change.getObject()));
    }

    @Override
    public <T, E> ListFieldAdd<T, E> caseListFieldAdd(ListFieldAdd<T, E> change) {
        return new ListFieldAdd<T, E>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          change.getIndex(), this.copyIfReference(change.getElement()));
    }

    @Override
    public <T> ListFieldClear<T> caseListFieldClear(ListFieldClear<T> change) {
        return new ListFieldClear<T>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName());
    }

    @Override
    public <T, E> ListFieldRemove<T, E> caseListFieldRemove(ListFieldRemove<T, E> change) {
        return new ListFieldRemove<T, E>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          change.getIndex(), this.copyIfReference(change.getElement()));
    }

    @Override
    public <T, E> ListFieldReplace<T, E> caseListFieldReplace(ListFieldReplace<T, E> change) {
        return new ListFieldReplace<T, E>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          change.getIndex(), this.copyIfReference(change.getOldValue()), this.copyIfReference(change.getNewValue()));
    }

    @Override
    public <T, K, V> MapFieldAdd<T, K, V> caseMapFieldAdd(MapFieldAdd<T, K, V> change) {
        return new MapFieldAdd<T, K, V>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          this.copyIfReference(change.getKey()), this.copyIfReference(change.getValue()));
    }

    @Override
    public <T> MapFieldClear<T> caseMapFieldClear(MapFieldClear<T> change) {
        return new MapFieldClear<T>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName());
    }

    @Override
    public <T, K, V> MapFieldRemove<T, K, V> caseMapFieldRemove(MapFieldRemove<T, K, V> change) {
        return new MapFieldRemove<T, K, V>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          this.copyIfReference(change.getKey()), this.copyIfReference(change.getValue()));
    }

    @Override
    public <T, K, V> MapFieldReplace<T, K, V> caseMapFieldReplace(MapFieldReplace<T, K, V> change) {
        return new MapFieldReplace<T, K, V>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          this.copyIfReference(change.getKey()), this.copyIfReference(change.getOldValue()),
          this.copyIfReference(change.getNewValue()));
    }

    @Override
    public <T, E> SetFieldAdd<T, E> caseSetFieldAdd(SetFieldAdd<T, E> change) {
        return new SetFieldAdd<T, E>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          this.copyIfReference(change.getElement()));
    }

    @Override
    public <T> SetFieldClear<T> caseSetFieldClear(SetFieldClear<T> change) {
        return new SetFieldClear<T>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName());
    }

    @Override
    public <T, E> SetFieldRemove<T, E> caseSetFieldRemove(SetFieldRemove<T, E> change) {
        return new SetFieldRemove<T, E>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          this.copyIfReference(change.getElement()));
    }

    @Override
    public <T, V> SimpleFieldChange<T, V> caseSimpleFieldChange(SimpleFieldChange<T, V> change) {
        return new SimpleFieldChange<T, V>(this.copyIfReference(change.getObject()), change.getStorageId(), change.getFieldName(),
          this.copyIfReference(change.getOldValue()), this.copyIfReference(change.getNewValue()));
    }

    @SuppressWarnings("unchecked")
    private <T> T copyIfReference(T obj) {
        return obj instanceof JObject ? (T)this.copy((JObject)obj) : obj;
    }

    /**
     * Copy the given {@link JObject} into the destination transaction.
     *
     * <p>
     * The implementation in {@link ChangeCopier} invokes {@code jobj.copyTo(this.dest, null)}, or
     * {@code jobj.copyTo(this.dest, null, (String[])null)} if this instance is configured to copy related objects.
     * If the given {@link JObject} does not exist, it is not copied (but the corresponding {@link JObject} is still returned).
     * Subclasses may override to copy additional or other objects referenced by {@code jobj} as needed.
     * </p>
     *
     * <p>
     * Note: this method is not invoked for a {@link ObjectDelete} change.
     * </p>
     *
     * @param jobj original object
     * @return copied object in {@link #dest}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    @SuppressWarnings("unchecked")
    protected JObject copy(JObject jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        return !jobj.exists() ? this.dest.getJObject(jobj) :
          this.copyRelated ? jobj.copyTo(this.dest, null, (String[])null) : jobj.copyTo(this.dest, null);
    }
}

