
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.schema.ComplexSchemaField;
import io.permazen.schema.SimpleSchemaField;

abstract class ComplexFieldIndex extends SimpleIndex {

    protected final int parentStorageId;
    protected final String subFieldName;
    protected final String parentFieldName;

    protected ComplexFieldIndex(JsckInfo info, int schemaVersion,
      ComplexSchemaField field, SimpleSchemaField subField, String parentFieldName, String subFieldName) {
        super(info, schemaVersion, subField);
        this.parentStorageId = field.getStorageId();
        this.parentFieldName = parentFieldName;
        this.subFieldName = subFieldName;
        assert this.parentStorageId > 0;
    }

    @Override
    public boolean isCompatible(Storage that) {
        if (!super.isCompatible(that))
            return false;
        if (this.parentStorageId != ((ComplexFieldIndex)that).parentStorageId)
            return false;
        return true;
    }

// Object

    @Override
    public final String toString() {
        return "index on " + this.subFieldName + " field #" + this.storageId
         + " (" + this.type + ") of " + this.parentFieldName + " field #" + this.parentStorageId;
    }
}

