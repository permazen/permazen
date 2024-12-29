
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.encoding.Encoding;
import io.permazen.util.ByteData;

import java.util.NavigableMap;
import java.util.Objects;

class MapValueIndex<K, V>
  extends ComplexSubFieldIndex<NavigableMap<K, V>, MapField<K, V>, V, io.permazen.core.MapValueIndex<K, V>> {

    protected final Encoding<?> keyEncoding;

    MapValueIndex(JsckInfo info, MapField<K, V> mapField) {
        super(info, mapField, mapField.getValueField());
        this.keyEncoding = io.permazen.core.Index.genericize(mapField.getKeyField().getEncoding());
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteData.Reader reader, ByteData indexValue, ObjId id) {

        // Decode map key
        final ByteData indexKeyValue = this.validateEncodedBytes(reader, this.keyEncoding);
        this.validateEOF(reader);

        // Validate value exists in map under specified key
        if (info.getConfig().isRepair()) {
            final ByteData.Writer writer = this.buildFieldKey(id, this.parentField.getStorageId());
            writer.write(indexKeyValue);
            final ByteData key = writer.toByteData();
            final ByteData actualValue = info.getKVStore().get(key);
            if (actualValue == null) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s value index does not contain indexed value %s key %s",
                  id, this.parentField, Jsck.ds(indexKeyValue)));
            } else if (!Objects.equals(actualValue, indexValue)) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s value index contains value %s != %s under key %s",
                  id, this.parentField, Jsck.ds(actualValue), Jsck.ds(indexValue), Jsck.ds(indexKeyValue)));
            }
        }
    }
}
