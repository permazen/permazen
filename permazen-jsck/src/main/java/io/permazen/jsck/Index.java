
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.Encodings;
import io.permazen.core.ObjId;
import io.permazen.encoding.Encoding;
import io.permazen.util.ByteData;

abstract class Index<I extends io.permazen.core.Index> extends Storage<I> {

    protected Index(JsckInfo info, I index) {
        super(info, index);
    }

    /**
     * Validate the encoding of an index entry and confirm that it agrees with the content of the corresponding object.
     *
     * @throws IllegalArgumentException if entry is invalid
     */
    public final void validateIndexEntry(JsckInfo info, ByteData.Reader reader) {
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
    protected abstract void validateIndexEntryContent(JsckInfo info, ByteData.Reader reader);

    /**
     * Validate the object simple field has the expected value. This assumes the object exists and has already been validated.
     *
     * @throws IllegalArgumentException if field does not have the expected value
     */
    protected void validateSimpleObjectField(JsckInfo info, ObjId id, int storageId, Encoding<?> encoding, ByteData expectedValue) {

        // If we are repairing, the key/value store will contain any corrections by this point, so it's safe to read the field
        if (info.getConfig().isRepair()) {
            final ByteData actualValue = info.getKVStore().get(this.buildFieldKey(id, storageId).toByteData());
            if (!expectedValue.equals(actualValue != null ? actualValue : encoding.getDefaultValueBytes()))
                throw new IllegalArgumentException(String.format("field value != %s", Jsck.ds(expectedValue)));
        }
    }

    @Override
    protected void validateObjectExists(JsckInfo info, ByteData.Reader reader, ObjId id) {
        try {
            super.validateObjectExists(info, reader, id);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("invalid index entry: %s", e.getMessage()));
        }
    }

    @Override
    protected <T> ByteData validateEncodedBytes(ByteData.Reader reader, Encoding<T> encoding) {
        try {
            return super.validateEncodedBytes(reader, encoding);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("invalid index entry: %s", e.getMessage()));
        }
    }

    @Override
    protected void validateEOF(ByteData.Reader reader) {
        try {
            super.validateEOF(reader);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("invalid index entry: %s", e.getMessage()));
        }
    }
}
