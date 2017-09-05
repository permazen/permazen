
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.base.Preconditions;

import io.permazen.core.FieldType;
import io.permazen.core.ObjId;
import io.permazen.kv.KeyRange;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.UnsignedIntEncoder;

import java.util.Arrays;

/**
 * Represents a storage ID used for stored information.
 */
abstract class Storage {

    protected final JsckInfo info;
    protected final int storageId;

    private int schemaVersion;

    protected Storage(JsckInfo info, int storageId) {
        Preconditions.checkArgument(info != null, "null info");
        Preconditions.checkArgument(storageId > 0, "non-positive storageId");
        this.info = info;
        this.storageId = storageId;
    }

    /**
     * Get one schema version where this storage is defined. Informational only - not included in {@link #equals equals()}.
     */
    public int getSchemaVersion() {
        return this.schemaVersion;
    }
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /**
     * Get corresponding storage ID.
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get key range.
     */
    public KeyRange getKeyRange() {
        return KeyRange.forPrefix(UnsignedIntEncoder.encode(this.storageId));
    }

    /**
     * Validate the object actually exists.
     *
     * <p>
     * If we are not repairing, this does nothing.
     */
    protected void validateObjectExists(JsckInfo info, ByteReader reader, ObjId id) {
        if (info.getConfig().isRepair() && info.getKVStore().get(id.getBytes()) == null)
            throw new IllegalArgumentException("object with " + id + " does not exist");
    }

    /**
     * Validate proper encoding of a field value.
     *
     * @return encoded field value
     */
    protected <T> byte[] validateEncodedBytes(ByteReader reader, FieldType<T> type) {

        // Decode value and capture bytes
        final T value;
        final int off = reader.getOffset();
        try {
            value = type.read(reader);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("can't decode value of " + type + " because value is truncated");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("can't decode value of " + type + ": " + e.getMessage());
        }
        final int len = reader.getOffset() - off;
        final byte[] bytes = reader.getBytes(off, len);

        // Verify correct and consistent encoding
        final ByteWriter writer = new ByteWriter(bytes.length);
        try {
            type.write(writer, value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("decoding then re-encoding value "
              + Jsck.ds(bytes) + " (" + value + ") using " + type + " throws " + e, e);
        }
        final byte[] bytes2 = writer.getBytes();
        if (!Arrays.equals(bytes2, bytes)) {
            throw new IllegalArgumentException("decoding then re-encoding value "
              + Jsck.ds(bytes) + " (" + value + ") using " + type + " results in" + " a different value " + Jsck.ds(bytes2));
        }

        // Done
        return bytes;
    }

    /**
     * Same as {@link #validateEncodedBytes validateEncodedBytes()} but returns the decoded value.
     *
     * @return decoded field value
     */
    protected <T> T validateEncodedValue(ByteReader reader, FieldType<T> type) {
        return type.read(new ByteReader(this.validateEncodedBytes(reader, type)));
    }

    protected void validateEOF(ByteReader reader) {
        if (reader.remain() > 0) {
            throw new IllegalArgumentException("value contains extra trailing garbage "
              + Jsck.ds(reader.getBytes(reader.getOffset())));
        }
    }

    /**
     * Instantiate a {@link ByteWriter} and write the given {@link ObjId} and field storage ID into it.
     */
    protected ByteWriter buildFieldKey(ObjId id, int storageId) {
        final ByteWriter writer = new ByteWriter();
        id.writeTo(writer);
        UnsignedIntEncoder.write(writer, storageId);
        return writer;
    }

// Object

    @Override
    public abstract String toString();

    public boolean isCompatible(Storage that) {
        if (this.storageId != that.storageId)
            return false;
        if (this.getClass() != that.getClass())
            return false;
        return true;
    }
}

