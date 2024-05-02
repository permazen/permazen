
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.Encodings;
import io.permazen.core.ObjId;
import io.permazen.encoding.Encoding;
import io.permazen.util.ByteReader;

import java.util.Arrays;

abstract class Index<I extends io.permazen.core.Index> extends Storage<I> {

    protected Index(JsckInfo info, I index) {
        super(info, index);
    }

    /**
     * Validate the encoding of an index entry and confirm that it agrees with the content of the corresponding object.
     *
     * @throws IllegalArgumentException if entry is invalid
     */
    public final void validateIndexEntry(JsckInfo info, ByteReader reader) {
        final int entryStorageId = this.validateEncodedValue(reader, Encodings.UNSIGNED_INT);
        assert entryStorageId == this.getStorageId();
        this.validateIndexEntryContent(info, reader);
    }

    /**
     * Validate the encoding of the content of an index entry following the storage ID and confirm that it agrees with
     * the content of the corresponding object. This assumes the object has already been validated.
     *
     * @throws IllegalArgumentException if entry is invalid
     */
    protected abstract void validateIndexEntryContent(JsckInfo info, ByteReader reader);

    /**
     * Validate the object simple field has the expected value. This assumes the object exists and has already been validated.
     *
     * @throws IllegalArgumentException if field does not have the expected value
     */
    protected void validateSimpleObjectField(JsckInfo info, ObjId id, int storageId, Encoding<?> encoding, byte[] expectedValue) {

        // If we are repairing, the key/value store will contain any corrections by this point, so it's safe to read the field
        if (info.getConfig().isRepair()) {
            final byte[] actualValue = info.getKVStore().get(this.buildFieldKey(id, storageId).getBytes());
            if (!Arrays.equals(expectedValue, actualValue != null ? actualValue : encoding.getDefaultValueBytes()))
                throw new IllegalArgumentException("field value != " + Jsck.ds(expectedValue));
        }
    }

    @Override
    protected void validateObjectExists(JsckInfo info, ByteReader reader, ObjId id) {
        try {
            super.validateObjectExists(info, reader, id);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid index entry: " + e.getMessage());
        }
    }

    @Override
    protected <T> byte[] validateEncodedBytes(ByteReader reader, Encoding<T> encoding) {
        try {
            return super.validateEncodedBytes(reader, encoding);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid index entry: " + e.getMessage());
        }
    }

    @Override
    protected void validateEOF(ByteReader reader) {
        try {
            super.validateEOF(reader);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid index entry: " + e.getMessage());
        }
    }
}
