
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.schema.SimpleSchemaField;
import org.jsimpledb.util.ByteReader;

class SimpleFieldIndex extends SimpleIndex {

    SimpleFieldIndex(JsckInfo info, int schemaVersion, SimpleSchemaField field) {
        super(info, schemaVersion, field);
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteReader reader, byte[] indexValue, ObjId id) {

        // No additional info
        this.validateEOF(reader);

        // Validate field value in object
        if (info.getConfig().isRepair())
            this.validateSimpleObjectField(info, id, this.storageId, this.type, indexValue);
    }

// Object

    @Override
    public String toString() {
        return "index on simple field #" + this.storageId + " having " + this.type;
    }
}

