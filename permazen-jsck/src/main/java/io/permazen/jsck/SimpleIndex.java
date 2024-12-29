
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.Encodings;
import io.permazen.core.ObjId;
import io.permazen.core.SimpleField;
import io.permazen.encoding.Encoding;
import io.permazen.util.ByteData;

abstract class SimpleIndex<T, I extends io.permazen.core.SimpleIndex<T>> extends Index<I> {

    protected final SimpleField<T> field;
    protected final Encoding<T> encoding;

    @SuppressWarnings("unchecked")
    SimpleIndex(JsckInfo info, SimpleField<T> field) {
        super(info, (I)field.getIndex());
        assert field.isIndexed();
        this.field = field;
        this.encoding = field.getIndex().getEncoding();
    }

    @Override
    protected final void validateIndexEntryContent(JsckInfo info, ByteData.Reader reader) {

        // Decode indexed value
        final ByteData value = this.validateEncodedBytes(reader, this.encoding);

        // Decode object ID
        final ObjId id = this.validateEncodedValue(reader, Encodings.OBJ_ID);

        // Validate object exists
        this.validateObjectExists(info, reader, id);

        // Proceed with subclass
        this.validateIndexEntrySuffix(info, reader, value, id);
    }

    /**
     * Validate the index entry content following the object ID (if any) and confirm that the entry agrees with
     * the content of the corresponding object. This assumes the object exists and has already been validated.
     *
     * @throws IllegalArgumentException if entry is invalid
     */
    protected abstract void validateIndexEntrySuffix(JsckInfo info, ByteData.Reader reader, ByteData indexValue, ObjId id);
}
