
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

class SimpleFieldStorageInfo<T> extends FieldStorageInfo {

    final FieldType<T> fieldType;
    final int superFieldStorageId;

    SimpleFieldStorageInfo(SimpleField<T> field, int superFieldStorageId) {
        this(field, field.fieldType, superFieldStorageId);
    }

    SimpleFieldStorageInfo(SimpleField<T> field, FieldType<T> fieldType, int superFieldStorageId) {
        super(field);
        this.fieldType = fieldType;
        this.superFieldStorageId = superFieldStorageId;
    }

    @Override
    public boolean isSubField() {
        return this.superFieldStorageId != 0;
    }

// Object

    @Override
    public String toString() {
        return "simple field with " + this.fieldType;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleFieldStorageInfo<?> that = (SimpleFieldStorageInfo<?>)obj;
        return this.fieldTypeEquals(that) && this.superFieldStorageId == that.superFieldStorageId;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.fieldTypeHashCode() ^ this.superFieldStorageId;
    }

    protected boolean fieldTypeEquals(SimpleFieldStorageInfo<?> that) {
        return this.fieldType.equals(that.fieldType);
    }

    protected int fieldTypeHashCode() {
        return this.fieldType.hashCode();
    }
}

