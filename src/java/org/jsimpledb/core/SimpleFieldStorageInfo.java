
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

class SimpleFieldStorageInfo extends FieldStorageInfo {

    final FieldType<?> fieldType;

    SimpleFieldStorageInfo(SimpleField<?> field, int superFieldStorageId) {
        super(field, superFieldStorageId);
        this.fieldType = field.fieldType;
    }

    @Override
    protected final void verifySharedStorageId(FieldStorageInfo other) {
        final SimpleFieldStorageInfo that = (SimpleFieldStorageInfo)other;
        this.verifySharedStorageId(that);
    }

    protected void verifySharedStorageId(SimpleFieldStorageInfo that) {
        if (!this.fieldType.equals(that.fieldType))
            throw new IllegalArgumentException("field types differ: " + this.fieldType + " != " + that.fieldType);
    }

    @Override
    public String toString() {
        return "simple field with type " + this.fieldType;
    }
}

