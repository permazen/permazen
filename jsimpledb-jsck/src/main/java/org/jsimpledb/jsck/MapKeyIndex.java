
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.schema.MapSchemaField;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

class MapKeyIndex extends ComplexFieldIndex {

    MapKeyIndex(JsckInfo info, int schemaVersion, MapSchemaField field) {
        super(info, schemaVersion, field, field.getKeyField(), "map", "key");
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteReader reader, byte[] indexKeyValue, ObjId id) {

        // No additional info
        this.validateEOF(reader);

        // Validate key exists in map
        if (info.getConfig().isRepair()) {
            final ByteWriter writer = this.buildFieldKey(id, this.parentStorageId);
            writer.write(indexKeyValue);
            final byte[] key = writer.getBytes();
            final byte[] actualValue = info.getKVStore().get(key);
            if (actualValue == null) {
                throw new IllegalArgumentException("object " + id + " map field #" + this.parentStorageId
                  + " with key " + this.type + " does not contain key " + Jsck.ds(indexKeyValue));
            }
        }
    }
}

