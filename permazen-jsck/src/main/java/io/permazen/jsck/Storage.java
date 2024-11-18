
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.base.Preconditions;

import io.permazen.core.Encodings;
import io.permazen.core.ObjId;
import io.permazen.encoding.Encoding;
import io.permazen.kv.KeyRange;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.util.Arrays;

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
    protected void validateObjectExists(JsckInfo info, ByteReader reader, ObjId id) {
        if (info.getConfig().isRepair() && info.getKVStore().get(id.getBytes()) == null)
            throw new IllegalArgumentException(String.format("object %s does not exist", id));
    }

    /**
     * Validate proper encoding of a field value.
     *
     * @return encoded field value
     */
    protected <T> byte[] validateEncodedBytes(ByteReader reader, Encoding<T> encoding) {

        // Decode value and capture bytes
        final T value;
        final int off = reader.getOffset();
        try {
            value = encoding.read(reader);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(
              "can't decode value of %s: %s", encoding, "value is truncated"));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
              "can't decode value of %s: %s", encoding, e.getMessage()));
        }
        final int len = reader.getOffset() - off;
        final byte[] bytes = reader.getBytes(off, len);

        // Verify correct and consistent encoding
        final ByteWriter writer = new ByteWriter(bytes.length);
        try {
            encoding.write(writer, value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
              "decode/encode error for value %s (%s) using %s: %s",
              Jsck.ds(bytes), value, encoding, String.format("encode threw %s", e)), e);
        }
        final byte[] bytes2 = writer.getBytes();
        if (!Arrays.equals(bytes2, bytes)) {
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
    protected <T> T validateEncodedValue(ByteReader reader, Encoding<T> encoding) {
        return encoding.read(new ByteReader(this.validateEncodedBytes(reader, encoding)));
    }

    protected void validateEOF(ByteReader reader) {
        if (reader.remain() > 0) {
            throw new IllegalArgumentException(String.format(
              "value contains extra trailing garbage %s", Jsck.ds(reader.getBytes(reader.getOffset()))));
        }
    }

    /**
     * Instantiate a {@link ByteWriter} and write the given {@link ObjId} and field storage ID into it.
     */
    protected ByteWriter buildFieldKey(ObjId id, int storageId) {
        final ByteWriter writer = new ByteWriter();
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
