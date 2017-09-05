
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.FieldTypeRegistry;
import io.permazen.core.ObjId;
import io.permazen.schema.ListSchemaField;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.UnsignedIntEncoder;

import java.util.Arrays;

class ListElementIndex extends CollectionElementIndex {

    ListElementIndex(JsckInfo info, int schemaVersion, ListSchemaField field) {
        super(info, schemaVersion, field, "list");
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteReader reader, byte[] indexValue, ObjId id) {

        // Decode list index
        final int listIndex = this.validateEncodedValue(reader, FieldTypeRegistry.UNSIGNED_INT);
        this.validateEOF(reader);

        // Validate element exists in list at specified index
        if (info.getConfig().isRepair()) {
            final ByteWriter writer = this.buildFieldKey(id, this.parentStorageId);
            UnsignedIntEncoder.write(writer, listIndex);
            final byte[] key = writer.getBytes();
            final byte[] actualValue = info.getKVStore().get(key);
            if (actualValue == null) {
                throw new IllegalArgumentException("object " + id + " list field #" + this.parentStorageId
                  + " with element " + this.type + " does not contain value " + Jsck.ds(indexValue) + " at index " + listIndex);
            } else if (!Arrays.equals(actualValue, indexValue)) {
                throw new IllegalArgumentException("object " + id + " list field #" + this.parentStorageId
                  + " with element " + this.type + "  contains value " + Jsck.ds(actualValue) + " != " + Jsck.ds(indexValue)
                  + " at index " + listIndex);
            }
        }
    }
}

