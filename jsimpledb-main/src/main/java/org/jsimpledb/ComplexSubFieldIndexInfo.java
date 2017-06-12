
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;

/**
 * Represents an index on a sub-field of a complex field.
 */
abstract class ComplexSubFieldIndexInfo extends SimpleFieldIndexInfo {

    private final int parentStorageId;

    ComplexSubFieldIndexInfo(JSimpleField jfield) {
        super(jfield);
        assert jfield.parent instanceof JComplexField;
        this.parentStorageId = jfield.getParentField().storageId;
    }

    /**
     * Get parent complex field storage info, if any.
     *
     * @return parent complex field storage ID, or zero if this instance does not represent a sub-field
     */
    public int getParentStorageId() {
        return this.parentStorageId;
    }

    /**
     * Recurse on this reference sub-field during a copy between transactions. Copies all objects referred to by
     * this sub-field in the given object from {@code srcTx} to {@code dstTx}.
     *
     * <p>
     * This method assumes that this indexed field is a reference field.
     *
     * @param copyState copy state
     * @param srcTx source transaction
     * @param dstTx destination transaction
     * @param id ID of the object containing the complex field and sub-field in {@code srcTx}
     * @param fieldIndex next index into {@code fieldIds}
     * @param fields fields to follow in the reference path
     */
    public void copyRecurse(CopyState copyState, JTransaction srcTx, JTransaction dstTx, ObjId id, int fieldIndex, int[] fields) {
        for (Object obj : this.iterateReferences(srcTx.tx, id)) {
            if (obj != null) {
                final ObjId ref = (ObjId)obj;
                srcTx.copyTo(copyState, dstTx, ref, ref, false, fieldIndex, fields);
            }
        }
    }

    /**
     * Iterate over the references in this sub-field in the given object.
     *
     * <p>
     * This method assumes that this indexed field is a reference field.
     */
    protected abstract Iterable<?> iterateReferences(Transaction tx, ObjId id);

// Object

    @Override
    protected String toStringPrefix() {
        return super.toStringPrefix()
          + ",parentStorageId=" + this.getParentStorageId();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ComplexSubFieldIndexInfo that = (ComplexSubFieldIndexInfo)obj;
        return this.parentStorageId == that.parentStorageId;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.parentStorageId;
    }
}

