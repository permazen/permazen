
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.ObjId;
import io.permazen.core.ObjIdEncoding;
import io.permazen.encoding.Encoding;
import io.permazen.schema.SchemaCompositeIndex;
import io.permazen.schema.SchemaObjectType;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteReader;

import java.util.Arrays;

class CompositeIndex extends Index {

    protected final int[] fieldStorageIds;
    protected final Encoding<?>[] encodings;

    CompositeIndex(JsckInfo info, int schemaVersion, SchemaObjectType objectType, SchemaCompositeIndex index) {
        super(info, index.getStorageId());
        this.fieldStorageIds = index.getIndexedFields().stream().mapToInt(Integer::intValue).toArray();
        this.encodings = new Encoding<?>[this.fieldStorageIds.length];
        for (int i = 0; i < this.encodings.length; i++) {
            final SimpleSchemaField field = (SimpleSchemaField)objectType.getSchemaFields().get(fieldStorageIds[i]);
            this.encodings[i] = this.info.findEncoding(schemaVersion, field).genericizeForIndex();
        }
    }

    @Override
    public boolean isCompatible(Storage that) {
        if (!super.isCompatible(that))
            return false;
        if (!Arrays.equals(this.fieldStorageIds, ((CompositeIndex)that).fieldStorageIds))
            return false;
        if (!Arrays.equals(this.encodings, ((CompositeIndex)that).encodings))
            return false;
        return true;
    }

    @Override
    protected void validateIndexEntryContent(JsckInfo info, ByteReader reader) {

        // Decode index entry
        final byte[][] values = new byte[this.encodings.length][];
        for (int i = 0; i < values.length; i++)
            values[i] = this.validateEncodedBytes(reader, this.encodings[i]);
        final ObjId id = this.validateEncodedValue(reader, new ObjIdEncoding());
        this.validateEOF(reader);

        // Validate field values in object
        for (int i = 0; i < values.length; i++)
            this.validateSimpleObjectField(info, id, this.fieldStorageIds[i], this.encodings[i], values[i]);
    }

// Object

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append("composite index on ");
        for (int i = 0; i < this.encodings.length; i++) {
            if (i > 0)
                buf.append(", ");
            buf.append("simple field #" + this.fieldStorageIds[i] + " having " + this.encodings[i]);
        }
        return buf.toString();
    }
}
