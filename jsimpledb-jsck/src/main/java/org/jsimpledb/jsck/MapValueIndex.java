
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import java.util.Arrays;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.schema.MapSchemaField;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

class MapValueIndex extends ComplexFieldIndex {

    protected final FieldType<?> keyType;

    MapValueIndex(JsckInfo info, int schemaVersion, MapSchemaField field) {
        super(info, schemaVersion, field, field.getValueField(), "map", "value");
        this.keyType = this.info.findFieldType(schemaVersion, field.getKeyField());
    }

    @Override
    public boolean isCompatible(Storage that) {
        if (!super.isCompatible(that))
            return false;
        if (!this.keyType.equals(((MapValueIndex)that).keyType))
            return false;
        return true;
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteReader reader, byte[] indexValue, ObjId id) {

        // Decode map key
        final byte[] indexKeyValue = this.validateEncodedBytes(reader, this.keyType);
        this.validateEOF(reader);

        // Validate value exists in map under specified key
        if (info.getConfig().isRepair()) {
            final ByteWriter writer = this.buildFieldKey(id, this.parentStorageId);
            writer.write(indexKeyValue);
            final byte[] key = writer.getBytes();
            final byte[] actualValue = info.getKVStore().get(key);
            if (actualValue == null) {
                throw new IllegalArgumentException("object " + id + " map field #" + this.parentStorageId + " with key "
                  + this.keyType + " does not contain key " + Jsck.ds(indexKeyValue));
            } else if (!Arrays.equals(actualValue, indexValue)) {
                throw new IllegalArgumentException("object " + id + " map field #" + this.parentStorageId + " with key "
                  + this.keyType + "  contains value " + Jsck.ds(actualValue) + " != " + Jsck.ds(indexValue) + " under key "
                  + Jsck.ds(indexKeyValue));
            }
        }
    }
}

