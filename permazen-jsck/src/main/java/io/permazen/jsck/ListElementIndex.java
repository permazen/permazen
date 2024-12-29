
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.Encodings;
import io.permazen.core.ListField;
import io.permazen.core.ObjId;
import io.permazen.util.ByteData;

import java.util.List;
import java.util.Objects;

class ListElementIndex<E> extends CollectionElementIndex<List<E>, ListField<E>, E, io.permazen.core.ListElementIndex<E>> {

    ListElementIndex(JsckInfo info, ListField<E> listField) {
        super(info, listField);
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteData.Reader reader, ByteData indexValue, ObjId id) {

        // Decode list index
        final int listIndex = this.validateEncodedValue(reader, Encodings.UNSIGNED_INT);
        this.validateEOF(reader);

        // Validate element exists in list at specified index
        if (info.getConfig().isRepair()) {
            final ByteData.Writer writer = this.buildFieldKey(id, this.parentField.getStorageId());
            Encodings.UNSIGNED_INT.write(writer, listIndex);
            final ByteData key = writer.toByteData();
            final ByteData actualValue = info.getKVStore().get(key);
            if (actualValue == null) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s element index does not contain indexed value %s at list index %d",
                  id, this.parentField, Jsck.ds(indexValue), listIndex));
            } else if (!Objects.equals(actualValue, indexValue)) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s element index contains value %s != %s at list index %d",
                  id, this.parentField, Jsck.ds(actualValue), Jsck.ds(indexValue), listIndex));
            }
        }
    }
}
