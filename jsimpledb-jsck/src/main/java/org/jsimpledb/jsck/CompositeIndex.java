
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import java.util.Arrays;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.FieldTypeRegistry;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.schema.SchemaCompositeIndex;
import org.jsimpledb.schema.SchemaObjectType;
import org.jsimpledb.schema.SimpleSchemaField;
import org.jsimpledb.util.ByteReader;

class CompositeIndex extends Index {

    protected final int[] fieldStorageIds;
    protected final FieldType<?>[] fieldTypes;

    CompositeIndex(JsckInfo info, int schemaVersion, SchemaObjectType objectType, SchemaCompositeIndex index) {
        super(info, index.getStorageId());
        this.fieldStorageIds = index.getIndexedFields().stream().mapToInt(Integer::intValue).toArray();
        this.fieldTypes = new FieldType<?>[this.fieldStorageIds.length];
        for (int i = 0; i < this.fieldTypes.length; i++) {
            final SimpleSchemaField field = (SimpleSchemaField)objectType.getSchemaFields().get(fieldStorageIds[i]);
            this.fieldTypes[i] = this.info.findFieldType(schemaVersion, field);
        }
    }

    @Override
    public boolean isCompatible(Storage that) {
        if (!super.isCompatible(that))
            return false;
        if (!Arrays.equals(this.fieldStorageIds, ((CompositeIndex)that).fieldStorageIds))
            return false;
        if (!Arrays.equals(this.fieldTypes, ((CompositeIndex)that).fieldTypes))
            return false;
        return true;
    }

    @Override
    protected void validateIndexEntryContent(JsckInfo info, ByteReader reader) {

        // Decode index entry
        final byte[][] values = new byte[this.fieldTypes.length][];
        for (int i = 0; i < values.length; i++)
            values[i] = this.validateEncodedBytes(reader, this.fieldTypes[i]);
        final ObjId id = this.validateEncodedValue(reader, FieldTypeRegistry.OBJ_ID);
        this.validateEOF(reader);

        // Validate field values in object
        for (int i = 0; i < values.length; i++)
            this.validateSimpleObjectField(info, id, this.fieldStorageIds[i], this.fieldTypes[i], values[i]);
    }

// Object

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append("composite index on ");
        for (int i = 0; i < this.fieldTypes.length; i++) {
            if (i > 0)
                buf.append(", ");
            buf.append("simple field #" + this.fieldStorageIds[i] + " having " + this.fieldTypes[i]);
        }
        return buf.toString();
    }
}

