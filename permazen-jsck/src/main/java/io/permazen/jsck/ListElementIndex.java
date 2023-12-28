
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.Encodings;
import io.permazen.core.ListField;
import io.permazen.core.ObjId;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.util.Arrays;
import java.util.List;

class ListElementIndex<E> extends CollectionElementIndex<List<E>, ListField<E>, E, io.permazen.core.ListElementIndex<E>> {

    ListElementIndex(JsckInfo info, ListField<E> listField) {
        super(info, listField);
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteReader reader, byte[] indexValue, ObjId id) {

        // Decode list index
        final int listIndex = this.validateEncodedValue(reader, Encodings.UNSIGNED_INT);
        this.validateEOF(reader);

        // Validate element exists in list at specified index
        if (info.getConfig().isRepair()) {
            final ByteWriter writer = this.buildFieldKey(id, this.parentField.getStorageId());
            Encodings.UNSIGNED_INT.write(writer, listIndex);
            final byte[] key = writer.getBytes();
            final byte[] actualValue = info.getKVStore().get(key);
            if (actualValue == null) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s element index does not contain indexed value %s at list index %d",
                  id, this.parentField, Jsck.ds(indexValue), listIndex));
            } else if (!Arrays.equals(actualValue, indexValue)) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s element index contains value %s != %s at list index %d",
                  id, this.parentField, Jsck.ds(actualValue), Jsck.ds(indexValue), listIndex));
            }
        }
    }
}
