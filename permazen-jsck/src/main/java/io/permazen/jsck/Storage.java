
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.base.Preconditions;

import io.permazen.core.Encodings;
import io.permazen.core.ObjId;
import io.permazen.encoding.Encoding;
import io.permazen.kv.KeyRange;
import io.permazen.util.ByteData;

import java.util.Objects;

/**
 * Represents a storage ID used for stored information.
 */
abstract class Storage<T extends io.permazen.core.SchemaItem> {

    protected final JsckInfo info;
    protected final T schemaItem;

    protected Storage(JsckInfo info, T schemaItem) {
        Preconditions.checkArgument(info != null, "null info");
        Preconditions.checkArgument(schemaItem != null, "null schemaItem");
        this.info = info;
        this.schemaItem = schemaItem;
    }

    /**
     * Get the schema item.
     */
    public T getSchemaItem() {
        return this.schemaItem;
    }

    /**
     * Get the storage ID.
     */
    public int getStorageId() {
        return this.schemaItem.getStorageId();
    }

    /**
     * Get key range.
     */
    public KeyRange getKeyRange() {
        return KeyRange.forPrefix(Encodings.UNSIGNED_INT.encode(this.getStorageId()));
    }

    /**
     * Verify that an object actually exists.
     *
     * <p>
     * If we are not repairing, this does nothing.
     */
    protected void validateObjectExists(JsckInfo info, ByteData.Reader reader, ObjId id) {
        if (info.getConfig().isRepair() && info.getKVStore().get(id.getBytes()) == null)
            throw new IllegalArgumentException(String.format("object %s does not exist", id));
    }

    /**
     * Validate proper encoding of a field value.
     *
     * @return encoded field value
     */
    protected <T> ByteData validateEncodedBytes(ByteData.Reader reader, Encoding<T> encoding) {

        // Decode value and capture bytes
        final T value;
        final int startingOffset = reader.getOffset();
        try {
            value = encoding.read(reader);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(
              "can't decode value of %s: %s", encoding, "value is truncated"));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
              "can't decode value of %s: %s", encoding, e.getMessage()));
        }
        final ByteData bytes = reader.getByteData().substring(startingOffset, reader.getOffset());

        // Verify correct and consistent encoding
        final ByteData.Writer writer = ByteData.newWriter(bytes.size());
        try {
            encoding.write(writer, value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
              "decode/encode error for value %s (%s) using %s: %s",
              Jsck.ds(bytes), value, encoding, String.format("encode threw %s", e)), e);
        }
        final ByteData bytes2 = writer.toByteData();
        if (!Objects.equals(bytes2, bytes)) {
            throw new IllegalArgumentException(String.format(
              "decode/encode error for value %s (%s) using %s: %s",
              Jsck.ds(bytes), value, encoding, String.format("got a different value %s", Jsck.ds(bytes2))));
        }

        // Done
        return bytes;
    }

    /**
     * Same as {@link #validateEncodedBytes validateEncodedBytes()} but returns the decoded value.
     *
     * @return decoded field value
     */
    protected <T> T validateEncodedValue(ByteData.Reader reader, Encoding<T> encoding) {
        return encoding.read(this.validateEncodedBytes(reader, encoding).newReader());
    }

    protected void validateEOF(ByteData.Reader reader) {
        if (reader.remain() > 0) {
            final ByteData garbage = reader.getByteData().substring(reader.getOffset());
            throw new IllegalArgumentException(String.format(
              "value contains extra trailing garbage %s", Jsck.ds(garbage)));
        }
    }

    /**
     * Instantiate a {@link ByteData.Writer} and write the given {@link ObjId} and field storage ID into it.
     */
    protected ByteData.Writer buildFieldKey(ObjId id, int storageId) {
        final ByteData.Writer writer = ByteData.newWriter();
        id.writeTo(writer);
        Encodings.UNSIGNED_INT.write(writer, storageId);
        return writer;
    }

// Object

    @Override
    public String toString() {
        return this.schemaItem.toString();
    }
}
