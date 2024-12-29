
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.ObjId;
import io.permazen.core.SimpleField;
import io.permazen.util.ByteData;

class SimpleFieldIndex<T, I extends io.permazen.core.SimpleFieldIndex<T>> extends SimpleIndex<T, I> {

    SimpleFieldIndex(JsckInfo info, SimpleField<T> field) {
        super(info, field);
    }

    @Override
    protected void validateIndexEntrySuffix(JsckInfo info, ByteData.Reader reader, ByteData indexValue, ObjId id) {

        // No additional info
        this.validateEOF(reader);

        // Validate field value in object
        if (info.getConfig().isRepair())
            this.validateSimpleObjectField(info, id, this.getStorageId(), this.encoding, indexValue);
    }
}
