
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

class SimpleFieldStorageInfo extends FieldStorageInfo {

    final FieldType<?> fieldType;
    final boolean hasComplexIndex;

    SimpleFieldStorageInfo(SimpleField<?> field, int superFieldStorageId, boolean hasComplexIndex) {
        super(field, superFieldStorageId);
        this.fieldType = field.fieldType;
        this.hasComplexIndex = hasComplexIndex;
    }

    /**
     * Get the {@link FieldType} associated with the {@link SimpleField}.
     */
    public FieldType<?> getFieldType() {
        return this.fieldType;
    }

    @Override
    public boolean canShareStorageId(StorageInfo obj) {
        if (!super.canShareStorageId(obj))
            return false;
        final SimpleFieldStorageInfo that = (SimpleFieldStorageInfo)obj;
        return this.fieldType.equals(that.fieldType);
    }

    @Override
    public String toString() {
        return "simple field with type " + this.fieldType;
    }
}

