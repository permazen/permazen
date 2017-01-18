
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

/**
 * Represents an index on a simple field, either a regular simple field or a sub-field of a complex field.
 */
abstract class SimpleFieldStorageInfo<T> extends IndexStorageInfo {

    final FieldType<T> fieldType;

    SimpleFieldStorageInfo(SimpleField<T> field) {
        super(field.storageId);
        this.fieldType = field.fieldType.genericizeForIndex();
    }

    CoreIndex<T, ObjId> getIndex(Transaction tx) {
        return new CoreIndex<>(tx, new IndexView<>(this.storageId, this.fieldType, FieldTypeRegistry.OBJ_ID));
    }

    /**
     * Remove all references from objects in the specified referrers set to the specified target through
     * the reference field associated with this instance. Used to implement {@link DeleteAction#UNREFERENCE}.
     *
     * <p>
     * This method may assume that this instance's {@link FieldType} is reference.
     *
     * @param tx transaction
     * @param target referenced object being deleted
     * @param referrers objects that refer to {@code target} via this reference field
     */
    abstract void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers);

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleFieldStorageInfo<?> that = (SimpleFieldStorageInfo<?>)obj;
        return this.fieldType.equals(that.fieldType);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.fieldType.hashCode();
    }
}

