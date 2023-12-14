
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
import io.permazen.util.CloseableIterator;
import io.permazen.util.UnsignedIntEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Constants and utility methods relating to the encoding and layout of a {@link Database} in a key/value store.
 *
 * <p>
 * The key/value space is divided into a data area and a meta-data area. The data area contains object data as
 * well as simple and composite index data. The meta-data area contains a recognizable signature, database format version,
 * all recorded schemas, the object schema index, and a range reserved for user applications.
 *
 * @see <a href="https://github.com/permazen/permazen/blob/master/LAYOUT.md">LAYOUT.md</a>
 */
public final class Layout {

    /**
     * The original {@link Database} layout format version #1.
     */
    public static final int FORMAT_VERSION_1 = 1;                                       // original format

    /**
     * The current {@link Database} layout format version ({@value #CURRENT_FORMAT_VERSION}).
     */
    public static final int CURRENT_FORMAT_VERSION = FORMAT_VERSION_1;

    /**
     * The single byte value that is a prefix of all meta-data keys.
     */
    private static final byte METADATA_PREFIX_BYTE = (byte)0x00;

    private static final byte[] METADATA_PREFIX = new byte[] { METADATA_PREFIX_BYTE };

    private static final byte[] FORMAT_VERSION_KEY = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0x00,
      (byte)'P', (byte)'e', (byte)'r', (byte)'m', (byte)'a', (byte)'z', (byte)'e', (byte)'n', (byte)0x00
    };

    private static final byte[] SCHEMA_TABLE_PREFIX = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0x01
    };

    private static final byte[] STORAGE_ID_TABLE_PREFIX = new byte[] {          // must immediately follow SCHEMA_TABLE_PREFIX
      METADATA_PREFIX_BYTE, (byte)0x02
    };

    private static final byte[] SCHEMA_INDEX_PREFIX = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0x80
    };

    private static final byte[] USER_META_DATA_KEY_PREFIX = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0xff
    };

    // Sanity check assumptions
    static {
        final byte[] schemaPrefix = Layout.getSchemaTablePrefix();
        final byte[] storageIdPrefix = Layout.getStorageIdTablePrefix();
        Preconditions.checkState(schemaPrefix.length == storageIdPrefix.length);
        Preconditions.checkState(Arrays.equals(storageIdPrefix, ByteUtil.getKeyAfterPrefix(schemaPrefix)));
    }

    private Layout() {
    }

    /**
     * Get the common prefix of all meta-data keys.
     *
     * @return meta-data prefix bytes
     */
    public static byte[] getMetaDataKeyPrefix() {
        return METADATA_PREFIX.clone();
    }

    /**
     * Get the key under which the database format version is encoded.
     *
     * <p>
     * The existence of this key also serves to identify a Permazen database.
     *
     * @return meta-data prefix bytes
     */
    public static byte[] getFormatVersionKey() {
        return FORMAT_VERSION_KEY.clone();
    }

    /**
     * Get the common prefix of all schema table keys.
     *
     * @return schema table key prefix
     */
    public static byte[] getSchemaTablePrefix() {
        return SCHEMA_TABLE_PREFIX.clone();
    }

    /**
     * Get the common prefix of all storage ID table keys.
     *
     * @return storage ID table key prefix
     */
    public static byte[] getStorageIdTablePrefix() {
        return STORAGE_ID_TABLE_PREFIX.clone();
    }

    /**
     * Get the key corresponding to an entry in an indexed table (e.g., schema table or storage ID table).
     *
     * @param prefix table key range prefix
     * @param index table index
     * @return key/value store key
     * @throws IllegalArgumentException if {@code index} is zero or negative
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static byte[] buildTableKey(byte[] prefix, int index) {
        Preconditions.checkArgument(prefix != null, "null prefix");
        Preconditions.checkArgument(index > 0, "index <= 0");
        final ByteWriter writer = new ByteWriter(prefix.length + UnsignedIntEncoder.encodeLength(index));
        writer.write(prefix);
        UnsignedIntEncoder.write(writer, index);
        return writer.getBytes();
    }

    /**
     * Get the common prefix of all object schema index entries.
     *
     * @return object schema index key prefix
     */
    public static byte[] getObjectSchemaIndexKeyPrefix() {
        return SCHEMA_INDEX_PREFIX.clone();
    }

    /**
     * Get the common prefix of all user-defined meta-data keys.
     *
     * @return user meta-data key prefix
     */
    public static byte[] getUserMetaDataKeyPrefix() {
        return USER_META_DATA_KEY_PREFIX.clone();
    }

    /**
     * Get a {@link CoreIndex} view of the object schema index in a key/value database.
     *
     * @param kv key/value data
     * @return object schema index
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public static CoreIndex<Integer, ObjId> getSchemaIndex(KVStore kv) {
        Preconditions.checkArgument(kv != null, "null kv");
        return new CoreIndex<>(kv, new IndexView<>(SCHEMA_INDEX_PREFIX, false, Encodings.UNSIGNED_INT, Encodings.OBJ_ID));
    }

    /**
     * Build the key for an object schema index entry.
     *
     * @param id object ID
     * @param schemaIndex object schema index
     * @return schemaIndex index entry key
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code schemaIndex} is non-positive
     */
    public static byte[] buildSchemaIndexKey(ObjId id, int schemaIndex) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(schemaIndex > 0, "non-positive schemaIndex");
        final ByteWriter writer = new ByteWriter(SCHEMA_INDEX_PREFIX.length
          + UnsignedIntEncoder.encodeLength(schemaIndex) + ObjId.NUM_BYTES);
        writer.write(SCHEMA_INDEX_PREFIX);
        UnsignedIntEncoder.write(writer, schemaIndex);
        id.writeTo(writer);
        return writer.getBytes();
    }

    /**
     * Decode schema XML from a schema table entry.
     *
     * @param reader compressed XML input
     * @return decoded schema model
     * @throws InvalidSchemaException if data or schema is invalid
     * @throws IllegalArgumentException if {@code value} is null
     */
    public static SchemaModel decodeSchema(ByteReader reader) {

        // Sanity check
        Preconditions.checkArgument(reader != null, "null reader");

        // Decompress and decode XML
        try (InflaterInputStream input = new InflaterInputStream(reader.asInputStream())) {
            return SchemaModel.fromXML(input);
        } catch (IOException e) {
            throw new InvalidSchemaException(String.format("error in compressed schema XML data: %s", e.getMessage()), e);
        }
    }

    /**
     * Encode schema XML for a schema table entry.
     *
     * @param writer compressed XML output
     * @param schemaModel schema model
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void encodeSchema(ByteWriter writer, SchemaModel schemaModel) {

        // Sanity check
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");

        // Encode and compress XML
        final Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        try (DeflaterOutputStream output = new DeflaterOutputStream(writer.asOutputStream(), deflater)) {
            schemaModel.toXML(output, false);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    /**
     * Delete all object and index data from the given {@link KVStore}.
     *
     * <p>
     * Upon return, the {@link KVStore} will still contain meta-data, but not any objects.
     *
     * @param kv key/value database
     */
    public static void deleteObjectData(KVStore kv) {

        // Get ranges
        final KeyRange metaDataRange = KeyRange.forPrefix(Layout.getMetaDataKeyPrefix());
        final KeyRange schemaIndexRange = KeyRange.forPrefix(Layout.getObjectSchemaIndexKeyPrefix());
        assert metaDataRange.contains(schemaIndexRange);

        // Delete everything except meta-data
        kv.removeRange(null, metaDataRange.getMin());
        kv.removeRange(metaDataRange.getMax(), null);

        // Delete the object schema index
        kv.removeRange(metaDataRange.getMin(), schemaIndexRange.getMin());
        kv.removeRange(schemaIndexRange.getMax(), metaDataRange.getMax());
    }

    /**
     * Copy non-object meta-data from one {@link KVStore} to another.
     *
     * <p>
     * This copies all meta-data except the object schema index. Any existing key/value pairs
     * in the destination meta-data range are not removed prior to the copy.
     *
     * @param src source key/value database
     * @param dst destination key/value database
     */
    public static void copyMetaData(KVStore src, KVStore dst) {

        // Get ranges
        final KeyRange metaDataRange = KeyRange.forPrefix(Layout.getMetaDataKeyPrefix());
        final KeyRange schemaIndexRange = KeyRange.forPrefix(Layout.getObjectSchemaIndexKeyPrefix());
        assert metaDataRange.contains(schemaIndexRange);

        // Copy meta-data
        Layout.copyRange(src, dst, metaDataRange.getMin(), schemaIndexRange.getMin());
        Layout.copyRange(src, dst, schemaIndexRange.getMax(), metaDataRange.getMax());
    }

    private static void copyRange(KVStore src, KVStore dst, byte[] minKey, byte[] maxKey) {
        try (CloseableIterator<KVPair> i = src.getRange(minKey, maxKey)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                dst.put(pair.getKey(), pair.getValue());
            }
        }
    }
}
