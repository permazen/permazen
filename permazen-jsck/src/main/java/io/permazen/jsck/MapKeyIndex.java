
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.util.NavigableMap;

class MapKeyIndex<K, V> extends ComplexSubFieldIndex<NavigableMap<K, V>, MapField<K, V>, K, io.permazen.core.MapKeyIndex<K, V>> {

    MapKeyIndex(JsckInfo info, MapField<K, V> mapField) {
        super(info, mapField, mapField.getKeyField());
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteReader reader, byte[] indexKeyValue, ObjId id) {

        // No additional info
        this.validateEOF(reader);

        // Validate key exists in map
        if (info.getConfig().isRepair()) {
            final ByteWriter writer = this.buildFieldKey(id, this.parentField.getStorageId());
            writer.write(indexKeyValue);
            final byte[] key = writer.getBytes();
            final byte[] actualValue = info.getKVStore().get(key);
            if (actualValue == null) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s key index does not contain indexed value %s",
                  id, this.parentField, Jsck.ds(indexKeyValue)));
            }
        }
    }
}
