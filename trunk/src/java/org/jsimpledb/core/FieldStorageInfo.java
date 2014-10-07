
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

abstract class FieldStorageInfo extends StorageInfo {

    final int superFieldStorageId;

    FieldStorageInfo(Field<?> field, int superFieldStorageId) {
        super(field.storageId);
        this.superFieldStorageId = superFieldStorageId;
    }

    /**
     * Get whether the associated field is a sub-field.
     */
    public boolean isSubField() {
        return this.superFieldStorageId != 0;
    }

    /**
     * Compare for compatability across schema versions.
     *
     * <p>
     * To be compatible, the two fields must be exactly the same in terms of binary encoding, Java representation,
     * and default value. Compatibililty does not include name or whether indexed.
     * </p>
     *
     * <p>
     * The implementation in {@link SimpleField} checks that the two fields have the same parent (object type
     * or super-field), and then delegates to {@link #verifySharedStorageId(FieldStorageInfo)}.
     * </p>
     *
     * @param obj other item to check for compatibility
     * @throws IllegalArgumentException if {@code obj} is not compatible with this instance
     */
    @Override
    public void verifySharedStorageId(StorageInfo obj) {
        super.verifySharedStorageId(obj);
        final FieldStorageInfo that = (FieldStorageInfo)obj;
        if (this.superFieldStorageId != that.superFieldStorageId) {
            throw new IllegalArgumentException("fields have different superfields: storage ID "
              + this.superFieldStorageId + " != " + that.superFieldStorageId);
        }
        this.verifySharedStorageId(that);
    }

    /**
     * Compare for compatability across schema versions.
     *
     * <p>
     * To be compatible, the two fields must be exactly the same in terms of binary encoding, Java representation,
     * and default value. Compatibililty does not include name or whether indexed.
     * </p>
     *
     * @param other other field to check for compatibility
     * @throws NullPointerException if {@code that} is null
     */
    protected abstract void verifySharedStorageId(FieldStorageInfo other);
}

