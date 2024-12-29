
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.Encodings;
import io.permazen.core.ObjId;
import io.permazen.util.ByteData;

class CompositeIndex extends Index<io.permazen.core.CompositeIndex> {

    CompositeIndex(JsckInfo info, io.permazen.core.CompositeIndex index) {
        super(info, index);
    }

    @Override
    protected void validateIndexEntryContent(JsckInfo info, ByteData.Reader reader) {

        // Decode index entry
        final io.permazen.core.CompositeIndex index = this.schemaItem;
        final int numFields = index.getFields().size();
        final ByteData[] values = new ByteData[numFields];
        for (int i = 0; i < values.length; i++)
            values[i] = this.validateEncodedBytes(reader, index.getEncodings().get(i));
        final ObjId id = this.validateEncodedValue(reader, Encodings.OBJ_ID);
        this.validateEOF(reader);

        // Validate field values in object
        for (int i = 0; i < values.length; i++) {
            this.validateSimpleObjectField(info, id,
              index.getFields().get(i).getStorageId(), index.getEncodings().get(i), values[i]);
        }
    }
}
