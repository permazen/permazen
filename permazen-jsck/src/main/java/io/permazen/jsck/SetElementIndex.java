
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.ObjId;
import io.permazen.core.SetField;
import io.permazen.util.ByteData;

import java.util.NavigableSet;

class SetElementIndex<E> extends CollectionElementIndex<NavigableSet<E>, SetField<E>, E, io.permazen.core.SetElementIndex<E>> {

    SetElementIndex(JsckInfo info, SetField<E> setField) {
        super(info, setField);
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteData.Reader reader, ByteData indexValue, ObjId id) {

        // No additional info
        this.validateEOF(reader);

        // Validate element exists in set
        if (info.getConfig().isRepair()) {
            final ByteData.Writer writer = this.buildFieldKey(id, this.parentField.getStorageId());
            writer.write(indexValue);
            final ByteData key = writer.toByteData();
            if (info.getKVStore().get(key) == null) {
                throw new IllegalArgumentException(String.format(
                  "object %s %s element index does not contain indexed value %s",
                  id, this.parentField, Jsck.ds(indexValue)));
            }
        }
    }
}
